package com.socrata.pg.store

import com.socrata.datacoordinator.truth.RowUserIdMap
import com.socrata.datacoordinator.truth.loader.sql.InspectedRow

/**
 * Reads rows
 */
trait RowReader[CT, CV] {
  def lookupRows(ids: Iterator[CV]):RowUserIdMap[CV, InspectedRow[CV]]
}
