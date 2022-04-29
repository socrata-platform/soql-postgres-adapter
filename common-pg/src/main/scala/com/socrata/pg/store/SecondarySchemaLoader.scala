package com.socrata.pg.store

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.loader.sql.{ChangeOwner, RepBasedPostgresSchemaLoader}
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.{FullTextSearch, Indexable}
import com.socrata.pg.error.{SqlErrorHandler, SqlErrorHelper, SqlErrorPattern}
import com.typesafe.scalalogging.{Logger => SLogger}
import java.sql.{Connection, PreparedStatement, Statement}
import java.security.MessageDigest
import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.soql.environment.ColumnName

class SecondarySchemaLoader[CT, CV](conn: Connection, dsLogger: Logger[CT, CV],
                                    repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT],
                                    tablespace: String => Option[String],
                                    fullTextSearch: FullTextSearch[CT],
                                    sqlErrorHandler: SqlErrorHandler,
                                    time: TimingReport) extends
  RepBasedPostgresSchemaLoader[CT, CV](conn, dsLogger, repFor, tablespace) {

  import SecondarySchemaLoader._

  override def addColumns(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    if (columnInfos.nonEmpty) super.addColumns(columnInfos)
    // createIndexes(columnInfos) was moved to publish time.
  }

  override def create(copyInfo: CopyInfo): Unit = {
    // we don't create audit or log tables because we don't need them.

    // TODO: this means we no longer keep copies of a dataset in the same tablespace.
    // This is a moot point right now, because we only support one copy.  When and if
    // we do so, we may need another solution for keeping copies in the same tablespace
    // depending on what tablespace/storage model we may adopt.
    val ts: Option[String] = tablespace(copyInfo.dataTableName)

    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + tablespaceSqlPart(ts) + ";" +
                   ChangeOwner.sql(conn, copyInfo.dataTableName))
    }
    dsLogger.workingCopyCreated(copyInfo)
  }

  def createFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    // Bit of a dance here because we only want to create the index if
    // it isn't already what it's supposed to be, and "what it's
    // supposed to be" is more complicated than for individual column
    // indices (e.g., if a new column is created, this index needs to
    // be recreated if it already exists).  In order to find out if we
    // need to do this, we'll use postgresql's comment facility to tag
    // the index with a hash of the SQL we're using to define the
    // index.
    //
    // Postgresql does give a way to get SQL for the index in the
    // pg_indexes table, but it's been normalized and it would be
    // silly to fight with PG implementation details to figure out if
    // our sql is identical to that SQL.
    //
    // This mainly matters so that the optimize call done before
    // publishing does not have all its hard work on the index thrown
    // away by the publish.
    if (columnInfos.nonEmpty) {
      val table = tableName(columnInfos)
      val idxname = s"idx_search_${table}"

      val sql = {
        val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
          throw new Exception(table + " does not exist when creating search index.")))
        fullTextSearch.searchVector(columnInfos.map(repFor).toSeq, None).flatMap { allColumnsVector =>
          using(conn.prepareStatement(directivesSql)) { (directivesStmt) =>
            val column = columnInfos.find(_.userColumnId.underlying == ":id")
            val shouldCreateSearchIndex = column.map(c => shouldCreateIndex(directivesStmt, c)).getOrElse(true)
            if (shouldCreateSearchIndex) {
              Some(s"CREATE INDEX ${idxname} on ${table} USING GIN ($allColumnsVector) $tablespace")
            } else {
              None
            }
          }
        }
      }

      sql match {
        case None =>
          dropFullTextSearchIndex(columnInfos)
        case Some(sql) =>
          val md = MessageDigest.getInstance("SHA-256")
          md.update(sql.getBytes(StandardCharsets.UTF_8))
          val tag = md.digest().iterator.map { b => "%02x".format(b & 0xff) }.mkString("")

          if(indexDefinitionHasChanged(idxname, tag)) {
            logger.info("creating fts index")
            dropFullTextSearchIndex(columnInfos)
            using(conn.createStatement()) { stmt =>
              time.info("create-search-index", "table" -> table) {
                SecondarySchemaLoader.fullTextIndexCreateSqlErrorHandler.guard(stmt) {
                  stmt.execute(sql)
                  // can't use a prepared statement parameter here, but the
                  // tag is just a hex string so...
                  stmt.execute(s"COMMENT ON INDEX $idxname is '$tag'")
                }
              }
            }
          }
      }
    }
  }

  def indexDefinitionHasChanged(indexName: String, tag: String): Boolean = {
    // relam is the oid of the index's access method; it will be 0 for non-indexes
    using(conn.prepareStatement("select obj_description(oid, 'pg_class') from pg_class where relname = ? and relam <> 0")) { stmt =>
      stmt.setString(1, indexName)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Option(rs.getString(1)) != Some(tag)
        } else {
          true
        }
      }
    }
  }

  def deoptimize(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    dropFullTextSearchIndex(columnInfos)
    dropIndexes(columnInfos)
  }

  def optimize(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    createFullTextSearchIndex(columnInfos)
    createIndexes(columnInfos)
  }

  protected def dropFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]): Unit =
    if (columnInfos.nonEmpty) {
      val table = tableName(columnInfos)
      using(conn.createStatement()) { (stmt: Statement) =>
        stmt.execute(s"DROP INDEX IF EXISTS idx_search_${table}")
      }
    }

  protected def dropIndexes(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    val table = tableName(columnInfos)
    using(conn.createStatement()) { stmt =>
      for {
        ci <- columnInfos
        indexSql <- repFor(ci).dropIndex(table)
      } stmt.execute(indexSql)
    }
  }

  /**
   * create indexes, also drop indexes that are not wanted.
   */
  def createIndexes(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    if (columnInfos.nonEmpty) {
      val table = tableName(columnInfos)
      val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
        throw new Exception(s"${table} does not exist when creating index.")))
      val size = columnInfos.size
      using(conn.createStatement(), conn.prepareStatement(directivesSql)) { (stmt, directivesStmt) =>
        for {
          (ci, idx) <- columnInfos.zipWithIndex
          createIndexSql <- repFor(ci).createIndex(table, tablespace)
          dropIndexSql <- repFor(ci).dropIndex(table)
        } {
          val createIndex = shouldCreateIndex(directivesStmt, ci)
          val action = (if (createIndex) "create" else "drop") + "-index"
          val sql = if (createIndex) createIndexSql else dropIndexSql
          time.info(action,
            "datasetId" -> ci.copyInfo.datasetInfo.systemId.underlying,
            "columnId" -> ci.userColumnId.underlying,
            "index" -> s"${idx}/${size}") {
            sqlErrorHandler.guard(conn) {
              stmt.execute(sql)
            }
          }
        }
      }
    }
  }

  /**
    * To suppress index create
    *   INSERT into index_directives(dataset_system_id, field_name, directives) VALUES (${datasetSystemId}, ${fieldName}, '{"enable": false}')
    *      (fieldName can be null to apply to all columns)
    * To trigger reindex
    *   curl -v  -X POST -H 'Content-Type: application/json' 'http://localhost:6010/dataset/${resourceName}/!secondary-reindex' --data '{}'
    */
  private def shouldCreateIndex(stmt: PreparedStatement, column: ColumnInfo[CT]): Boolean = {
    val defaultResult = true
    stmt.setLong(1, column.copyInfo.systemId.underlying)
    stmt.setLong(2, column.systemId.underlying)
    using(stmt.executeQuery()) { resultSet =>
      if (resultSet.next()) {
        Option(resultSet.getString("directive")) match {
          case Some(json) =>
            JsonUtil.parseJson[JObject](json) match {
              case Right(JObject(indexDirectives)) =>
                indexDirectives.get("enabled") match {
                  case Some(JBoolean(b)) => b
                  case _ => defaultResult
                }
              case Left(_) => defaultResult
            }
          case None => defaultResult
        }
      } else {
        defaultResult
      }
    }
  }

  private def tableName(columnInfo: Iterable[ColumnInfo[CT]]) = columnInfo.head.copyInfo.dataTableName

  private def tablespaceSqlPart(tablespace: Option[String]): String = {
    tablespace.map(" TABLESPACE " + _).getOrElse("")
  }
}

object SecondarySchemaLoader {
  private val logger = SLogger[SecondarySchemaLoader[_, _]]

  val fullTextIndexCreateSqlErrorHandler = new SqlErrorHandler {
    // Class 54 — Program Limit Exceeded
    //   54000 	program_limit_exceeded
    //   54001 	statement_too_complex
    //   54011 	too_many_columns
    //   54023 	too_many_arguments
    // Example:
    // PSQLException: SQLState = 54000, detailMessage = ERROR: row is too big: size 17880, maximum size 8160
    // PSQLException: SQLState = 54000, detailMessage = ERROR: string is too long for tsvector (1099438 bytes, max 1048575 bytes)
    private val rowIsTooBig = new SqlErrorHelper(SqlErrorPattern("54000", " is too (big|long)".r))
    def guard(conn: Connection)(f: => Unit): Unit = rowIsTooBig.guard(conn, None)(f)
  }

  private val directivesSql = "SELECT directive FROM index_directive_map WHERE copy_system_id =? AND column_system_id =? AND deleted_at is null limit 1"
}
