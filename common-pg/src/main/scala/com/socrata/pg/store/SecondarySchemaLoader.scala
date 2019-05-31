package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.{ChangeOwner, RepBasedPostgresSchemaLoader}
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.{FullTextSearch, IndexDirectives, Indexable}
import com.socrata.pg.error.{SqlErrorHandler, SqlErrorHelper, SqlErrorPattern}
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Connection, PreparedStatement, Statement}

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.soql.environment.ColumnName

class SecondarySchemaLoader[CT, CV](conn: Connection, dsLogger: Logger[CT, CV],
                                    repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT],
                                    tablespace: String => Option[String],
                                    fullTextSearch: FullTextSearch[CT],
                                    sqlErrorHandler: SqlErrorHandler) extends
  RepBasedPostgresSchemaLoader[CT, CV](conn, dsLogger, repFor, tablespace) with Logging {

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

  def createFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]): Unit =
    if (columnInfos.nonEmpty) {
      dropFullTextSearchIndex(columnInfos)
      val table = tableName(columnInfos)
      val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
        throw new Exception(table + " does not exist when creating search index.")))
      logger.info("creating fts index")
      fullTextSearch.searchVector(columnInfos.map(repFor).toSeq, None) match {
        case None => // nothing to do
        case Some(allColumnsVector) =>
          using(conn.createStatement(), conn.prepareStatement(directivesSql)) { (stmt: Statement,  directivesStmt) =>
            val ci = columnInfos.head
            if (shouldCreateIndex(directivesStmt, ci.copyInfo.datasetInfo.systemId, Some(ColumnName(":fts")))) {
              SecondarySchemaLoader.fullTextIndexCreateSqlErrorHandler.guard(stmt) {
                stmt.execute(s"CREATE INDEX idx_search_${table} on ${table} USING GIN ($allColumnsVector) $tablespace")
              }
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

  def createIndexes(columnInfos: Iterable[ColumnInfo[CT]]): Unit = {
    if (columnInfos.nonEmpty) {
      val table = tableName(columnInfos)
      val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
        throw new Exception(s"${table} does not exist when creating index.")))
      using(conn.createStatement(), conn.prepareStatement(directivesSql)) { (stmt, directivesStmt) =>
        for {
          (ci, idx) <- columnInfos.zipWithIndex
          createIndexSql <- repFor(ci).createIndex(table, tablespace) if
            shouldCreateIndex(directivesStmt, ci.copyInfo.datasetInfo.systemId, ci.fieldName)
        } {
          logger.info("creating index {} {}", ci.userColumnId.underlying, idx.toString)
          sqlErrorHandler.guard(conn) {
            stmt.execute(createIndexSql)
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
  private def shouldCreateIndex(stmt: PreparedStatement, datasetId: DatasetId, fieldName: Option[ColumnName]): Boolean = {
    fieldName.flatMap { field =>
      stmt.setLong(1, datasetId.underlying)
      stmt.setString(2, field.name)
      val resultSet = stmt.executeQuery()
      if (resultSet.next()) {
        Option(resultSet.getString("directives")) match {
          case Some(json) =>
            JsonUtil.parseJson[IndexDirectives](json) match {
              case Right(indexDirectives) =>
                Some(indexDirectives.enable)
              case Left(_) => None
            }
          case None => None
        }
      } else {
        None
      }
    }.getOrElse(true)
  }

  private def tableName(columnInfo: Iterable[ColumnInfo[CT]]) = columnInfo.head.copyInfo.dataTableName

  private def tablespaceSqlPart(tablespace: Option[String]): String = {
    tablespace.map(" TABLESPACE " + _).getOrElse("")
  }
}

object SecondarySchemaLoader {
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

  private val directivesSql = "SELECT directives FROM index_directives WHERE dataset_system_id =? AND (field_name = ? OR field_name is null) order by field_name nulls last limit 1"
}
