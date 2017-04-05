package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.pg.store.{RollupManager, PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Handles WorkingCopyCreated Event
 */
case class WorkingCopyCreatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                     datasetId: Option[com.socrata.datacoordinator.id.DatasetId],
                                     datasetInfo: SecondaryDatasetInfo,
                                     copyInfo: SecondaryCopyInfo) extends Logging {
  copyInfo.copyNumber match {
    case 1 =>
      logger.info(s"create first copy {}", datasetInfo.internalName)
      val (copyInfoSecondary: TruthCopyInfo, sLoader) = createDataset(pgu, datasetInfo.localeName, datasetInfo.resourceName)
      pgu.secondaryDatasetMapWriter.createInternalNameMapping(
        datasetInfo.internalName,
        copyInfoSecondary.datasetInfo.systemId
      )
    case copyNumBeyondOne: Long =>

      pgu.datasetMapReader.datasetInfo(datasetId.get).map { truthDatasetInfo =>
        logger.info("create working copy {} {} {}",
          datasetInfo.internalName, truthDatasetInfo.toString, copyNumBeyondOne.toString)
        val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
        val copyIdFromSequence = pgu.secondaryDatasetMapWriter.allocateCopyId()
        val newCopyInfo = pgu.datasetMapWriter.unsafeCreateCopy(
          truthDatasetInfo,
          copyIdFromSequence,
          copyNumBeyondOne,
          com.socrata.datacoordinator.truth.metadata.LifecycleStage.valueOf(copyInfo.lifecycleStage.toString),
          copyInfo.dataVersion)
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        sLoader.create(newCopyInfo)

        // Copy rollups
        val rm = new RollupManager(pgu, newCopyInfo)
        pgu.datasetMapReader.rollups(truthCopyInfo).foreach { ru =>
          val sru = RollupManager.makeSecondaryRollupInfo(ru)
          RollupCreatedOrUpdatedHandler(pgu, newCopyInfo, sru)
        }
        pgu.datasetMapReader.rollups(newCopyInfo).foreach { ru =>
          rm.updateRollup(ru, newCopyInfo.dataVersion)
        }
      }
  }

  private def createDataset(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            locale: String, resourceName: Option[String]): (TruthCopyInfo, SchemaLoader[SoQLType]) = {
    val copyInfo = pgu.datasetMapWriter.create(locale, resourceName)
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(copyInfo)
    (copyInfo, sLoader)
  }
}
