package com.socrata.pg.store.events

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.pg.store.{RollupManager, PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class CopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {
  val rm = new RollupManager(pgu, copyInfo)
  rm.dropRollups(immediate = false)
  pgu.datasetMapWriter.deleteRollupRelationships(copyInfo)
  pgu.datasetMapWriter.deleteRollupRelationshipByRollupMapCopyInfo(copyInfo)
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

object WorkingCopyPublishedHandler {
  def apply(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: CopyInfo): CopyInfo = {
    val (publishedCopyInfo, _) = pgu.datasetMapWriter.publish(truthCopyInfo)
    publishedCopyInfo
  }
}

case class DataCopiedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                             truthDatasetInfo: DatasetInfo,
                             truthCopyInfo: CopyInfo) {
  pgu.datasetMapReader.published(truthDatasetInfo) match {
    case Some(publishedCopy) =>
      val copier = pgu.datasetContentsCopier(pgu.logger(truthDatasetInfo, "_no_user")) // TODO: fill in proper user
      val publishedSchema = pgu.datasetMapReader.schema(publishedCopy)
      val publishedCtx = new DatasetCopyContext[SoQLType](publishedCopy, publishedSchema)
      val newSchema = pgu.datasetMapReader.schema(truthCopyInfo)
      val newCopyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, newSchema)
      copier.copy(publishedCtx, newCopyCtx)
    case None => throw new ResyncSecondaryException("DataCopy triggers resync datas %s copy-%s data-ver-%s".format(
        truthDatasetInfo.systemId, truthCopyInfo.copyNumber, truthCopyInfo.dataVersion
      ))
  }
}
