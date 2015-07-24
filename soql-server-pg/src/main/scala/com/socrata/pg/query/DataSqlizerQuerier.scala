package com.socrata.pg.query

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.{MutableRow, Row}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.loader.sql.AbstractRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.pg.soql.ParametricSql
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Connection, ResultSet, SQLException}

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] => ()
  private val sqlFetchSize = 1000

  def query(conn: Connection, analysis: SoQLAnalysis[UserColumnId, CT],
            toSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
            toRowCountSql: (SoQLAnalysis[UserColumnId, CT], String) => ParametricSql, // analsysis, tableName
            reqRowCount: Boolean,
            querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]]):
    CloseableIterator[Row[CV]] with RowCount = {

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

    // For some weird reason, when you iterate over the querySchema, a new Rep is created from scratch
    // every time, which is very expensive.  Move that out of the inner loop of decodeRow.
    // Also, the orderedMap is extremely inefficient and very complex to debug.
    // TODO: refactor PG server not to use Ordered Map.
    val decoders = querySchema.map { case (cid, rep) =>
      (cid, rep.fromResultSet _, rep.physColumns.length)
    }.toArray

    // get rows
    if (analysis.selection.size > 0) {
      val rs = executeSql(conn, toSql(analysis, dataTableName))
      // Statement and resultset are closed by the iterator.
      new ResultSetIt(rowCount, rs, decodeRow(decoders))
    } else {
      logger.debug("Queried a dataset with no user columns")
      EmptyIt
    }
  }

  def decodeRow(decoders: Array[(ColumnId, (ResultSet, Int) => CV, Int)])(rs: ResultSet): Row[CV] = {
    val row = new MutableRow[CV]
    var i = 1

    decoders.foreach { case (cid, rsExtractor, physColumnsLen) =>
      row(cid) = rsExtractor(rs, i)
      i += physColumnsLen
    }
    row.freeze()
  }

  private def executeSql(conn: Connection, pSql: ParametricSql): ResultSet = {
    try {
      logger.debug("sql: {}", pSql.sql)
      // Statement to be closed by caller
      val stmt = conn.prepareStatement(pSql.sql)
      // need to explicitly set a fetch size to stream results
      stmt.setFetchSize(sqlFetchSize)
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

  class ResultSetIt(val rowCount: Option[Long], rs: ResultSet, toRow: (ResultSet) => Row[CV])
    extends CloseableIterator[Row[CV]] with RowCount {

    private var nextOpt: Option[Boolean] = None

    def hasNext: Boolean = {
      nextOpt = nextOpt match {
        case Some(b) => nextOpt
        case None => Some(rs.next())
      }
      nextOpt.get
    }

    def next(): Row[CV] = {
      if (hasNext) {
        val row = toRow(rs)
        nextOpt = None
        row
      } else {
        throw new Exception("No more data for the iterator.")
      }
    }

    def close(): Unit = {
      try {
        rs.getStatement.close()
      } finally {
        rs.close()
      }
    }
  }

  object EmptyIt extends CloseableIterator[Nothing] with RowCount {
    val rowCount = Some(0L)
    def hasNext: Boolean = false
    def next(): Nothing = throw new Exception("Called next() on an empty iterator")
    def close(): Unit = {}
  }
}

trait RowCount {
  val rowCount: Option[Long]
}
