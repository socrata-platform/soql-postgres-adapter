package com.socrata.pg.store

import com.socrata.datacoordinator.truth.RowUserIdMap
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.{DataSqlizer, InspectedRow}
import com.socrata.datacoordinator.util.TransferrableContextTimingReport
import java.sql.Connection

/**
 *  Reads rows from a postgres secondary store
 */
class PGSecondaryRowReader[CT, CV](val connection:Connection, val sqlizer: DataSqlizer[CT, CV], val timingReport: TransferrableContextTimingReport) extends RowReader[CT, CV] {
  val datasetContext = sqlizer.datasetContext

  def lookupRows(ids: Iterator[CV]): RowUserIdMap[CV, InspectedRow[CV]] =
    timingReport("lookup-rows") {
      using(sqlizer.findRows(connection, ids)) { it =>
        val result = datasetContext.makeIdMap[InspectedRow[CV]]()
        for(row <- it.flatten) result.put(row.id, row)
        result
      }
    }
}
