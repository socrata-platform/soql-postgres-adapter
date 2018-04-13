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

  /**
    * The maximum number of rows we want to have to keep in memory at once.  The JDBC driver
    * uses it to determine how many results to fetch from the cursor at once.  If we know the
    * number of results is less than this (eg. LIMIT) then we omit setting the fetch size
    * so we can skip using a cursor and enable parallel query support in pg9.6.
    * The particular value comes from starting at 1000, our default limit and previous fixed
    * fetch size, and rounding to 1024 in case anyone is using powers of two then adding one
    * for possible off by one errors or attempts to see if there is another page of results.
    */
  private val sqlFetchSize = 1025

  def query(conn: Connection, analyses: Seq[SoQLAnalysis[UserColumnId, CT]],
            toSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql, // analsysis, tableName
            toRowCountSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql, // analsysis, tableName
            reqRowCount: Boolean,
            querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]],
            queryTimeout: Option[Duration],
            debug: Boolean):
    CloseableIterator[Row[CV]] with RowCount = {

    // get row count
    val rowCount: Option[Long] =
      if (reqRowCount) {
        val rowCountSql = toRowCountSql(analyses, dataTableName)
        // We know this will only return one row, so fetchsize of 0 is ok
        using(executeSql(conn, rowCountSql, queryTimeout, 0, debug)) { rs =>
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

    /*
     * We need to set a fetchSize on the statement to limit the amount of data we are buffering in memory at
     * once.  Unfortunately, this currently prevents parallel query execution in postgres because it uses cursors.
     * Since the major benefit of parallel queries is for queries that take a lot of rows and reduce them to a small number,
     * if we can tell from the query that it can't return more than sqlFetchSize rows, we can safely set the fetchsize
     * to 0 and get the benefits of parallel queries.
     */
    val fetchSize = analyses.last.limit match {
      case Some(n) if (n <= sqlFetchSize) => 0
      case _ => sqlFetchSize
    }

    // get rows
    if (analyses.exists(_.selection.size > 0)) {
      val sql = toSql(analyses, dataTableName)
      val rs = executeSql(conn, sql, queryTimeout, fetchSize, debug)
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

  private def executeSql(conn: Connection, pSql: ParametricSql, timeout: Option[Duration], fetchSize: Integer, debug: Boolean): ResultSet = {
    try {
      if (timeout.isDefined && timeout.get.isFinite()) {
        val ms = timeout.get.toMillis.min(Int.MaxValue).max(1).toInt.toString
        logger.trace(s"Setting statement timeout to ${ms}ms")
        execute(conn, setTimeout(ms))
      }
      logger.debug("sql: {}", pSql.sql)
      if (debug) { logger.info(pSql.toString) }
      // Statement to be closed by caller
      val stmt = conn.prepareStatement(pSql.sql.head)
      // need to explicitly set a fetch size to stream results
      stmt.setFetchSize(fetchSize)
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
