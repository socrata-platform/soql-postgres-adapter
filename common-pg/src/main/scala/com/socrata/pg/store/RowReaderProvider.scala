package com.socrata.pg.store

import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext

/**
 * Factory for providing things which can read rows
 */
trait RowReaderProvider[CT, CV] {
  def reader(copyCtx: DatasetCopyContext[CT]): RowReader[CT, CV]
}
