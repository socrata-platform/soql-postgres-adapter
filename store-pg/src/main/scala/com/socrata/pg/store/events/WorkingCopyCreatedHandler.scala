package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.pg.store.{IndexManager, PGSecondaryLogger, PGSecondaryUniverse, RollupManager}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger
import com.socrata.pg.config.DbType

object WorkingCopyCreatedHandler {
  private val logger = Logger[WorkingCopyCreatedHandler]
}

/**
 * Handles WorkingCopyCreated Event
 * Unlike columns which are copied (replayed) from truth store where no work about columns is needed in this handle,
 * rollups and indexes are copied from secondary store in this handler.
 * TODO: index_directives are similar to rollups and indexes.  But because they depend on columns which have not been
 *       copied yet, index_directives copy should be done on the publish event.
 */
case class WorkingCopyCreatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                     datasetId: Option[com.socrata.datacoordinator.id.DatasetId],
                                     datasetInfo: SecondaryDatasetInfo,
                                     copyInfo: SecondaryCopyInfo)(implicit val dbType: DbType) {
  import WorkingCopyCreatedHandler.logger

  copyInfo.copyNumber match {
    case 1 =>
      logger.info(s"create first copy {}", datasetInfo.internalName)
      val (copyInfoSecondary: TruthCopyInfo, sLoader) = createDataset(pgu, datasetInfo.localeName, datasetInfo.resourceName)
      pgu.datasetMapWriter.createInternalNameMapping(
        datasetInfo.internalName,
        copyInfoSecondary.datasetInfo.systemId
      )
    case copyNumBeyondOne: Long =>

      pgu.datasetMapReader.datasetInfo(datasetId.get).map { truthDatasetInfo =>
        logger.info("create working copy {} {} {}",
          datasetInfo.internalName, truthDatasetInfo.toString, copyNumBeyondOne.toString)
        val oldCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
        val copyIdFromSequence = pgu.datasetMapWriter.allocateCopyId()
        val newCopyInfo = pgu.datasetMapWriter.unsafeCreateCopy(
          truthDatasetInfo,
          copyIdFromSequence,
          copyNumBeyondOne,
          com.socrata.datacoordinator.truth.metadata.LifecycleStage.valueOf(copyInfo.lifecycleStage.toString),
          copyInfo.dataVersion,
          copyInfo.dataVersion)
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        sLoader.create(newCopyInfo)

        // Copy rollups
        val rm = new RollupManager(pgu, newCopyInfo)
        pgu.datasetMapReader.rollups(oldCopyInfo).foreach { ru =>
          val sru = RollupManager.makeSecondaryRollupInfo(ru)
          RollupCreatedOrUpdatedHandler(pgu, newCopyInfo, sru)
        }
        pgu.datasetMapReader.rollups(newCopyInfo).foreach { ru =>
          rm.updateRollup(ru, Some(oldCopyInfo), Function.const(false))
        }

        // Copy indexes
        val im = new IndexManager(pgu, newCopyInfo)
        pgu.datasetMapReader.indexes(oldCopyInfo).foreach { idx =>
          val sidx = im.makeSecondaryIndexInfo(idx)
          IndexCreatedOrUpdatedHandler(pgu, newCopyInfo, sidx)
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
