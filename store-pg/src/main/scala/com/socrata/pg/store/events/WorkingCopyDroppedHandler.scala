package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo => TruthDatasetInfo}


case class WorkingCopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: TruthDatasetInfo) {
  pgu.datasetMapReader.unpublished(datasetInfo) match {
    case Some(copyInfo) =>
      CopyDroppedHandler(pgu, copyInfo)
    case None =>
      throw new Exception("Drop unpublished copy that does not exist.")
  }
}
