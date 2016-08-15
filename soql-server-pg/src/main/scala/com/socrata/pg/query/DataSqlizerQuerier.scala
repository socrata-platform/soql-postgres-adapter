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
import scala.concurrent.duration.Duration
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Connection, ResultSet, SQLException}

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] => ()
  private val sqlFetchSize = 1000

  def query(conn: Connection, analyses: Seq[SoQLAnalysis[UserColumnId, CT]],
            toSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql, // analsysis, tableName
            toRowCountSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql, // analsysis, tableName
            reqRowCount: Boolean,
            querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]],
            queryTimeout: Option[Duration]):
    CloseableIterator[Row[CV]] with RowCount = {

    // get row count
    val rowCount: Option[Long] =
      if (reqRowCount) {
        val rowCountSql = toRowCountSql(analyses, dataTableName)
        using(executeSql(conn, rowCountSql, queryTimeout)) { rs =>
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
    if (analyses.exists(_.selection.size > 0)) {
      val rs = executeSql(conn, toSql(analyses, dataTableName), queryTimeout)
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

  def execute(conn: Connection, s: String) {
    using(conn.createStatement()) { stmt =>
      logger.trace("Executing simple SQL {}", s)
      stmt.execute(s)
    }
  }

  def setTimeout(timeoutMs: String) = s"SET LOCAL statement_timeout TO $timeoutMs"
  def resetTimeout = "SET LOCAL statement_timeout TO DEFAULT"

  private def executeSql(conn: Connection, pSql: ParametricSql, timeout: Option[Duration]): ResultSet = {
    try {
      if (timeout.isDefined && timeout.get.isFinite()) {
        val ms = timeout.get.toMillis.min(Int.MaxValue).max(1).toInt.toString
        logger.trace(s"Setting statement timeout to ${ms}ms")
        execute(conn, setTimeout(ms))
      }
      logger.debug("sql: {}", pSql.sql)
      // Statement to be closed by caller
      val stmt = conn.prepareStatement(pSql.sql.head)
      // need to explicitly set a fetch size to stream results
      stmt.setFetchSize(sqlFetchSize)
      pSql.setParams.zipWithIndex.foreach { case (setParamFn, idx) =>
        setParamFn(Some(stmt), idx + 1)
      }
      stmt.executeQuery()
    } catch {
      case ex: SQLException =>
        logger.error(s"SQL Exception (${ex.getSQLState}) with timeout=$timeout on $pSql")
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
