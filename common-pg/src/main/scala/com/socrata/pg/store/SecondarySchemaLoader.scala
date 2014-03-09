package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.RepBasedPostgresSchemaLoader
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.{FullTextSearch, Indexable}
import java.sql.{Statement, Connection}

class SecondarySchemaLoader[CT, CV](conn: Connection, logger: Logger[CT, CV],
                                    repFor: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT],
                                    tablespace: String => Option[String],
                                    fullTextSearch: FullTextSearch[CT]) extends
  RepBasedPostgresSchemaLoader[CT, CV](conn, logger, repFor, tablespace) {

  override def addColumns(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return; // ok? copied from parent schema loader
    super.addColumns(columnInfos)
    createIndexes(columnInfos)
  }

  def createFullTextSearchIndex(columnInfos: Iterable[ColumnInfo[CT]]) {
    if(columnInfos.isEmpty) return
    val table = tableName(columnInfos)
    fullTextSearch.searchVector(columnInfos.map(repFor).toSeq) match {
      case Some(allColumnsVector) =>
        using(conn.createStatement()) { (stmt: Statement) =>
          stmt.execute(s"DROP INDEX IF EXISTS idx_search_${table}")
          stmt.execute(s"CREATE INDEX idx_search_${table} on ${table} USING GIN ($allColumnsVector)")
        }
      case None => // nothing to do
    }
  }

  protected def createIndexes(columnInfos: Iterable[ColumnInfo[CT]]) {
    val table = tableName(columnInfos)
    using(conn.createStatement()) { stmt =>
      for {
        ci <- columnInfos
        createIndexSql <- repFor(ci).createIndex(table)
      } stmt.execute(createIndexSql)
    }
  }

  private def tableName(columnInfo: Iterable[ColumnInfo[CT]]) = columnInfo.head.copyInfo.dataTableName
}
