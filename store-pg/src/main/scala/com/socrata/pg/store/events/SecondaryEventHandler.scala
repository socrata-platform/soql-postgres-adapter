package com.socrata.pg.store.events

import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, DatasetInfo => TruthDatasetInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
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

case class DataCopiedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthDatasetInfo: TruthDatasetInfo, truthCopyInfo: TruthCopyInfo) {
  pgu.datasetMapReader.published(truthDatasetInfo) match {
    case Some(publishedCopy) =>
      val copier = pgu.datasetContentsCopier(pgu.logger(truthDatasetInfo, "_no_user")) // TODO: fill in proper user
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)
      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, schema)
      copier.copy(publishedCopy, copyCtx)
    case None =>
      val msg = s"DataCopy triggers resync dataset ${truthDatasetInfo.systemId} copy-${truthCopyInfo.copyNumber} data-ver-${truthCopyInfo.dataVersion}"
      throw new ResyncSecondaryException(msg)
  }
}