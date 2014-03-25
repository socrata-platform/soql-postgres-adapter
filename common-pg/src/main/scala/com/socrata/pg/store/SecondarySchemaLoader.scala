package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.RepBasedPostgresSchemaLoader
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.{FullTextSearch, Indexable}
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Statement, Connection}
import com.socrata.pg.error.SqlErrorHandler

class SecondarySchemaLoader[CT, CV](conn: Connection, dsLogger: Logger[CT, CV],
                                    repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT],
                                    tablespace: String => Option[String],
                                    fullTextSearch: FullTextSearch[CT],
                                    sqlErrorHandler: SqlErrorHandler) extends
  RepBasedPostgresSchemaLoader[CT, CV](conn, dsLogger, repFor, tablespace) with Logging {

  override def addColumns(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return; // ok? copied from parent schema loader
    super.addColumns(columnInfos)
    createIndexes(columnInfos)
  }

  def createFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return
    dropFullTextSearchIndex(columnInfos)
    val table = tableName(columnInfos)
    fullTextSearch.searchVector(columnInfos.map(repFor).toSeq) match {
      case Some(allColumnsVector) =>
        using(conn.createStatement()) { (stmt: Statement) =>
          stmt.execute(s"CREATE INDEX idx_search_${table} on ${table} USING GIN ($allColumnsVector)")
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

  protected def createIndexes(columnInfos: Iterable[ColumnInfo[CT]]) {
    val table = tableName(columnInfos)
    using(conn.createStatement()) { stmt =>
      for {
        ci <- columnInfos
        createIndexSql <- repFor(ci).createIndex(table)
      } {
        sqlErrorHandler.guard(conn) {
          stmt.execute(createIndexSql)
        }
      }
    }
  }

  private def tableName(columnInfo: Iterable[ColumnInfo[CT]]) = columnInfo.head.copyInfo.dataTableName
}
