package com.socrata.pg.query

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.AbstractRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.MutableRow
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.pg.soql.ParametricSql
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{SQLException, PreparedStatement, Connection, ResultSet}

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] =>

  def query(conn: Connection, analysis: SoQLAnalysis[UserColumnId, CT],
               toSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
               toRowCountSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
               reqRowCount: Boolean,
               querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]]) :
               CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount = {

    // get row count
    val rowCount: Option[Long] =
      if (reqRowCount) {
        using(executeSql(conn, toRowCountSql(analysis, dataTableName))) { rs =>
          try {
            rs.next()
            Some(rs.getLong(1))
          } finally {
            rs.getStatement.close()
          }
        }
      } else {
        None
      }

    // get rows
    if (analysis.selection.size > 0) {
      val rs = executeSql(conn, toSql(analysis, dataTableName))
      // Statement and resultset are closed by the iterator.
      new ResultSetIt(rowCount, rs, decodeRow(querySchema))
    } else {
      logger.debug("Queried a dataset with no user columns")
      EmptyIt
    }
  }

  def decodeRow(schema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]])(rs: ResultSet): com.socrata.datacoordinator.Row[CV] = {

    val row = new MutableRow[CV]
    var i = 1

    schema.foreach { case (cid, rep) =>
      row(cid) = rep.fromResultSet(rs, i)
      i += rep.physColumns.length
    }
    row.freeze()
  }

  private def executeSql(conn: Connection, pSql: ParametricSql): ResultSet = {
    try {
      logger.debug("sql: {}", pSql.sql)
      // Statement to be closed by caller
      val stmt = conn.prepareStatement(pSql.sql)
      // need to explicitly set a fetch size to stream results
      stmt.setFetchSize(1000)
      pSql.setParams.zipWithIndex.foreach { case (setParamFn, idx) =>
        setParamFn(Some(stmt), idx + 1)
      }
      stmt.executeQuery()
    } catch {
      case ex: SQLException =>
        logger.error(s"SQL Exception on ${pSql}")
        throw ex
    }
  }

  class ResultSetIt(val rowCount: Option[Long], rs: ResultSet, toRow: (ResultSet) => com.socrata.datacoordinator.Row[CV])
    extends CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount {

    private var _hasNext: Option[Boolean] = None

    def hasNext: Boolean = {
      _hasNext = _hasNext match {
        case Some(b) => _hasNext
        case None => Some(rs.next())
      }
      _hasNext.get
    }

    def next(): com.socrata.datacoordinator.Row[CV] = {
      if (hasNext) {
        val row = toRow(rs)
        _hasNext = None
        row
      } else {
        throw new Exception("No more data for the iterator.")
      }
    }

    def close() {
      try {
        rs.getStatement().close()
      } finally {
        rs.close()
      }
    }
  }

  object EmptyIt extends CloseableIterator[Nothing] with RowCount {
    val rowCount = Some(0L)
    def hasNext = false
    def next() = throw new Exception("Called next() on an empty iterator")
    def close() {}
  }
}

trait RowCount {

  val rowCount: Option[Long]

}
