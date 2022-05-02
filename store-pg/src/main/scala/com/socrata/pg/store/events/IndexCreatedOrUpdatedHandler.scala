package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.secondary.{IndexInfo => SecIndexInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, IndexInfo}
import com.socrata.pg.store.{IndexManager, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}

/**
 * Handles creates or updates to index tables.  Note that this handles only the metadata, it doesn't actually rebuild the indexs.
 */
object IndexCreatedOrUpdatedHandler {
  def apply(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo, secIndexInfo: SecIndexInfo): IndexInfo = {
    val indexName = new IndexName(secIndexInfo.name)
    pgu.datasetMapReader.index(copyInfo, indexName) match {
      case Some(index) =>
        pgu.datasetMapWriter.dropIndex(copyInfo, Some(indexName))
        val im = new IndexManager(pgu, copyInfo)
        im.dropIndex(index)
      case None =>
        // nothing to do
    }
    pgu.datasetMapWriter.createOrUpdateIndex(copyInfo, indexName, secIndexInfo.expressions, secIndexInfo.filter)
  }
}
