package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.{SqlLogger, RepBasedPostgresSchemaLoader}
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator, SqlColumnRep}
import com.socrata.pg.store.index.{FullTextSearch, Indexable}
import com.socrata.pg.error.{SqlErrorPattern, SqlErrorHandler, SqlErrorHelper}
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Statement, Connection}

class SecondarySchemaLoader[CT, CV](conn: Connection, dsLogger: Logger[CT, CV],
                                    repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT],
                                    tablespace: String => Option[String],
                                    fullTextSearch: FullTextSearch[CT],
                                    sqlErrorHandler: SqlErrorHandler) extends
  RepBasedPostgresSchemaLoader[CT, CV](conn, dsLogger, repFor, tablespace) with Logging {

  override def addColumns(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return; // ok? copied from parent schema loader
    super.addColumns(columnInfos)
    // createIndexes(columnInfos) was moved to publish time.
  }

  override def create(copyInfo: CopyInfo) {
    // we don't create audit or log tables because we don't need them.

    // TODO: this means we no longer keep copies of a dataset in the same tablespace.
    // This is a moot point right now, because we only support one copy.  When and if
    // we do so, we may need another solution for keeping copies in the same tablespace
    // depending on what tablespace/storage model we may adopt.
    val ts: Option[String] =
      tablespace(copyInfo.dataTableName)

    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + tablespaceSqlPart(ts))
    }
    dsLogger.workingCopyCreated(copyInfo)
  }

  def createFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return
    dropFullTextSearchIndex(columnInfos)
    val table = tableName(columnInfos)
    val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
      throw new Exception(table + " does not exist when creating search index.")))
    fullTextSearch.searchVector(columnInfos.map(repFor).toSeq) match {
      case Some(allColumnsVector) =>
        using(conn.createStatement()) { (stmt: Statement) =>
          SecondarySchemaLoader.fullTextIndexCreateSqlErrorHandler.guard(stmt) {
            stmt.execute(s"CREATE INDEX idx_search_${table} on ${table} USING GIN ($allColumnsVector) $tablespace")
          }
        }
      case None => // nothing to do
    }
  }

  def deoptimize(columnInfos: Iterable[ColumnInfo[CT]]) {
    dropFullTextSearchIndex(columnInfos)
    dropIndexes(columnInfos)
  }

  def optimize(columnInfos: Iterable[ColumnInfo[CT]]) {
    createFullTextSearchIndex(columnInfos)
    createIndexes(columnInfos)
  }

  protected def dropFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return
    val table = tableName(columnInfos)
    using(conn.createStatement()) { (stmt: Statement) =>
      stmt.execute(s"DROP INDEX IF EXISTS idx_search_${table}")
    }
  }

  protected def dropIndexes(columnInfos: Iterable[ColumnInfo[CT]]) {
    val table = tableName(columnInfos)
    using(conn.createStatement()) { stmt =>
      for {
        ci <- columnInfos
        indexSql <- repFor(ci).dropIndex(table)
      } stmt.execute(indexSql)
    }
  }

  def createIndexes(columnInfos: Iterable[ColumnInfo[CT]]) {
    if (columnInfos.nonEmpty) {
      val table = tableName(columnInfos)
      val tablespace = tablespaceSqlPart(tablespaceOfTable(table).getOrElse(
        throw new Exception(table + " does not exist when creating index.")))
      dropIndexes(columnInfos)
      using(conn.createStatement()) { stmt =>
        for {
          ci <- columnInfos
          createIndexSql <- repFor(ci).createIndex(table, tablespace)
        } {
          sqlErrorHandler.guard(conn) {
            stmt.execute(createIndexSql)
          }
        }
      }
    }
  }

  private def tableName(columnInfo: Iterable[ColumnInfo[CT]]) = columnInfo.head.copyInfo.dataTableName

  private def tablespaceSqlPart(tablespace: Option[String]): String = {
    tablespace.map(" TABLESPACE " + _).getOrElse("")
  }
}

object SecondarySchemaLoader {

  val fullTextIndexCreateSqlErrorHandler = new SqlErrorHandler {

    // Example: ERROR: row is too big: size 17880, maximum size 8160
    private val rowIsTooBig = new SqlErrorHelper(SqlErrorPattern("54000", "^row is too big".r))

    def guard(conn: Connection)(f: => Unit) {
      rowIsTooBig.guard(conn, None)(f)
    }
  }
}