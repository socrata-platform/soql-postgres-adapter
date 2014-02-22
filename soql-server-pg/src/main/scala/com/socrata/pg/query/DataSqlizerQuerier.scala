package com.socrata.pg.query

import com.socrata.datacoordinator.truth.loader.sql.AbstractRepBasedDataSqlizer
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.MutableRow
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.SoQLAnalysis
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Connection, ResultSet}

trait DataSqlizerQuerier[CT, CV] extends AbstractRepBasedDataSqlizer[CT, CV] with Logging {
  this: AbstractRepBasedDataSqlizer[CT, CV] =>

  def query(conn: Connection, analysis: SoQLAnalysis[UserColumnId, CT],
               toSql: (SoQLAnalysis[UserColumnId, CT], String) => String, // analsysis, tableName
               systemToUserColumnMap: Map[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.id.UserColumnId],
               userToSystemColumnMap: Map[com.socrata.datacoordinator.id.UserColumnId, com.socrata.datacoordinator.id.ColumnId],
               querySchema: ColumnIdMap[SqlColumnRep[CT, CV]]) :
               CloseableIterator[com.socrata.datacoordinator.Row[CV]] = {

    val sql = toSql(analysis, this.dataTableName)
    val stmt = conn.prepareStatement(sql) // To be closed on closeable iterator close.
    val rs = stmt.executeQuery()
    new ResultSetIt(rs, decodeRow(querySchema))
  }

  def decodeRow(schema: ColumnIdMap[SqlColumnRep[CT, CV]])(rs: ResultSet): com.socrata.datacoordinator.Row[CV] = {

    val row = new MutableRow[CV]
    var i = 1

    schema.keys.toSeq.reverse.foreach { cid => // TODO: Remove the need for reverse
      val rep = schema(cid)
      row(cid) = rep.fromResultSet(rs, i)
      i += rep.physColumns.length
    }
    row.freeze()
  }

  class ResultSetIt(rs: ResultSet, toRow: (ResultSet) => com.socrata.datacoordinator.Row[CV]) extends CloseableIterator[com.socrata.datacoordinator.Row[CV]] {

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
}
