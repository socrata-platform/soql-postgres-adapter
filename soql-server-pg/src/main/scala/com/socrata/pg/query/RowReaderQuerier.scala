package com.socrata.pg.query

import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.loader.sql.DataSqlizer
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.pg.soql.ParametricSql
import com.socrata.pg.store.PGSecondaryRowReader
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.SoQLAnalysis
import scala.concurrent.duration.Duration

trait RowReaderQuerier[CT, CV] {
  this: PGSecondaryRowReader[CT, CV] => ()

  def query(analyses: Seq[SoQLAnalysis[UserColumnId, CT]],
            toSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql,
            toRowCountSql: (Seq[SoQLAnalysis[UserColumnId, CT]], String) => ParametricSql, // analsysis, tableName
            reqRowCount: Boolean,
            querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]],
            queryTimeout: Option[Duration]):
            Managed[CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount] = {
    val sqlizerq = sqlizer.asInstanceOf[DataSqlizer[CT, CV] with DataSqlizerQuerier[CT, CV]]
    val resultIter = sqlizerq.query(connection, analyses, toSql, toRowCountSql, reqRowCount, querySchema, queryTimeout)
    managed(resultIter)
  }

  def getSqlReps(systemToUserColumnMap: Map[ColumnId, UserColumnId]): Map[UserColumnId, SqlColumnRep[CT, CV]] = {
    val sqlizerq = sqlizer.asInstanceOf[DataSqlizer[CT, CV] with DataSqlizerQuerier[CT, CV]]
    val userColumnIdRepMap = sqlizerq.repSchema.foldLeft(Map.empty[UserColumnId, SqlColumnRep[CT, CV]]) {
      (map, colIdAndsqlRep) =>
        colIdAndsqlRep match {
          case (columnId, sqlRep) => map + (systemToUserColumnMap(columnId) -> sqlRep)
        }
    }
    userColumnIdRepMap
  }
}
