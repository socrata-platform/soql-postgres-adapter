package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.secondary.{IndexInfo => SecIndexInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, IndexInfo}
import com.socrata.pg.store.{IndexManager, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}

/**
 * Handles creation or updates to index tables.  Note that this handles only the metadata, it doesn't
 * actually rebuild the indexs.  That is done separately at a higher level because they need rebuilding
 * when (almost) anything changes.
 */
object IndexCreatedOrUpdatedHandler {
  def apply(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
            copyInfo: CopyInfo,
            secIndexInfo: SecIndexInfo): IndexInfo = {
    pgu.datasetMapReader.index(copyInfo, new IndexName (secIndexInfo.name)) match {
      case Some(index) =>
        pgu.datasetMapWriter.dropIndex(copyInfo, Some(new IndexName(secIndexInfo.name) ) )
        val im = new IndexManager(pgu, copyInfo)
        im.dropIndex(index)
      case None =>
    }
    val index = pgu.datasetMapWriter.createOrUpdateIndex (copyInfo, new IndexName (secIndexInfo.name), secIndexInfo.expressions, secIndexInfo.filter)
    index
  }
}
