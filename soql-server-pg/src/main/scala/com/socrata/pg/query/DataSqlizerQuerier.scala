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
import java.sql.{PreparedStatement, Connection, ResultSet}

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] =>

  def query(conn: Connection, analysis: SoQLAnalysis[UserColumnId, CT],
               toSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
               toRowCountSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
               reqRowCount: Boolean,
               systemToUserColumnMap: Map[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.id.UserColumnId],
               userToSystemColumnMap: Map[com.socrata.datacoordinator.id.UserColumnId, com.socrata.datacoordinator.id.ColumnId],
               querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]]) :
               CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount = {

    // get row count
    val rowCount: Option[Long] =
      if (reqRowCount) {
        val ParametricSql(sql, setParams) = toRowCountSql(analysis, this.dataTableName)
        logger.debug("rowcount sql: {}", sql)
        using(conn.prepareStatement(sql)) { stmt: PreparedStatement =>
          setParams.zipWithIndex.foreach { case (setParamFn, idx) => setParamFn(Some(stmt), idx + 1) }
          val rs = stmt.executeQuery()
          rs.next()
          Some(rs.getLong(1))
        }
      } else {
        None
      }

    // get rows
    if (analysis.selection.size > 0) {
      val ParametricSql(sql, setParams) = toSql(analysis, this.dataTableName)
      logger.debug("sql: {}", sql)
      val stmt = conn.prepareStatement(sql) // To be closed on closeable iterator close.
      setParams.zipWithIndex.foreach { case (setParamFn, idx) =>
        setParamFn(Some(stmt), idx + 1)
      }
      val rs = stmt.executeQuery()
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
