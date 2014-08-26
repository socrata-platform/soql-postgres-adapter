package com.socrata.pg.store.events

import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, DatasetInfo => TruthDatasetInfo}
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class CopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: TruthCopyInfo) {
  pgu.datasetMapWriter.dropCopy(copyInfo)
}

case class TruncateHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: TruthCopyInfo) {
  pgu.truncator.truncate(copyInfo, pgu.logger(copyInfo.datasetInfo, "not-used"))
}

case class WorkingCopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: TruthDatasetInfo) {
  pgu.datasetMapReader.unpublished(datasetInfo) match {
    case Some(copyInfo) =>
      CopyDroppedHandler(pgu, copyInfo)
    case None =>
      throw new Exception("Drop unpublished copy that does not exist.")
  }
}

case class WorkingCopyPublishedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo) {
  pgu.datasetMapWriter.publish(truthCopyInfo)
}
