package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.pg.store.{IndexManager, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}

/**
 * Drops the index metadata for the given copy.
 */
case class IndexDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                               copyInfo: CopyInfo,
                               indexName: IndexName) {
  val im = new IndexManager(pgu, copyInfo)
  im.dropIndex(indexName)
  pgu.datasetMapWriter.dropIndex(copyInfo, Some(indexName))
}
