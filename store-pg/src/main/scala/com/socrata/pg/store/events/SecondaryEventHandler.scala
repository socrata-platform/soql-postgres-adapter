package com.socrata.pg.store.events

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.pg.store.{RollupManager, PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class CopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {
  val rm = new RollupManager(pgu, copyInfo)
  rm.dropRollups(immediate = false)
  pgu.datasetMapWriter.dropCopy(copyInfo)
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
  sLoader.drop(copyInfo)
}

case class TruncateHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {
  pgu.truncator.truncate(copyInfo, pgu.logger(copyInfo.datasetInfo, "not-used"))
}

case class WorkingCopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: DatasetInfo) {
  pgu.datasetMapReader.unpublished(datasetInfo) match {
    case Some(copyInfo) =>
      CopyDroppedHandler(pgu, copyInfo)
    case None =>
      throw new Exception("Drop unpublished copy that does not exist.")
  }
}

case class WorkingCopyPublishedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: CopyInfo) {
  pgu.datasetMapWriter.publish(truthCopyInfo)
}

case class DataCopiedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                             truthDatasetInfo: DatasetInfo,
                             truthCopyInfo: CopyInfo) {
  pgu.datasetMapReader.published(truthDatasetInfo) match {
    case Some(publishedCopy) =>
      val copier = pgu.datasetContentsCopier(pgu.logger(truthDatasetInfo, "_no_user")) // TODO: fill in proper user
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)
      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, schema)
      copier.copy(publishedCopy, copyCtx)
    case None => throw new ResyncSecondaryException("DataCopy triggers resync datas %s copy-%s data-ver-%s".format(
        truthDatasetInfo.systemId, truthCopyInfo.copyNumber, truthCopyInfo.dataVersion
      ))
  }
}
