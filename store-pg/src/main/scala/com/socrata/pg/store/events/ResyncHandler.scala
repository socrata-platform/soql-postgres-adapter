package com.socrata.pg.store.events

import com.rojoma.simplearm._
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, ColumnInfo => SecondaryColumnInfo, RollupInfo, DatasetInfo, LifecycleStage}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage => TruthLifecycleStage, CopyInfo => TruthCopyInfo, DatasetCopyContext, ColumnInfo}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PGSecondaryLogger, RollupManager, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.typesafe.scalalogging.slf4j.Logging

case class ResyncHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                         secondaryDatasetInfo: DatasetInfo,
                         secondaryCopyInfo: SecondaryCopyInfo) extends Logging {

  // scalastyle:ignore method.length cyclomatic.complexity
  def doResync(newSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
               rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
               rollups: Seq[RollupInfo],
               resyncBatchSize: Int,
               updateRollups: (TruthCopyInfo => Unit)): Unit =
  {
    val truthCopyInfo = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName) match { // scalastyle:ignore line.size.limit
      case None =>
        // The very top record - dataset_internal_name_map is missing.  Start everything new from new dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest
        //      SET broken_at = null,
        //          latest_secondary_data_version = 0,
        //          latest_secondary_lifecycle_stage = 'Unpublished'
        //    WHERE dataset_system_id = ? -- 20
        //   DELETE from falth.dataset_internal_name_map
        //    WHERE dataset_internal_name = ? -- 'alpha.20'
        logger.info("re-creating secondary dataset with new id")
        val newCopyInfo = pgu.datasetMapWriter.create(secondaryDatasetInfo.localeName)
        val newDatasetId = newCopyInfo.datasetInfo.systemId
        logger.info("new secondary dataset {} {}", secondaryDatasetInfo.internalName, newDatasetId.toString())
        pgu.secondaryDatasetMapWriter.createInternalNameMapping(secondaryDatasetInfo.internalName, newDatasetId)
        newCopyInfo
      case Some(dsId) =>
        // Find the very top record - dataset_internal_name_map.
        // Delete and recreate the copy with the same dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest
        //      SET broken_at = null,
        //          latest_secondary_data_version = 0,
        //          latest_secondary_lifecycle_stage = 'Unpublished'
        //    WHERE dataset_system_id = ? -- 20
        pgu.datasetMapReader.datasetInfo(dsId).map { truthDatasetInfo =>
          pgu.datasetMapWriter.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber) match {
            case Some(copyInfo) =>
              logger.info(
                "delete existing copy so that a new one can be created with the same ids {} {}",
                truthDatasetInfo.systemId.toString(),
                secondaryCopyInfo.copyNumber.toString
              )
              val rm = new RollupManager(pgu, copyInfo)
              rm.dropRollups(immediate = true) // drop all rollup tables
              pgu.secondaryDatasetMapWriter.deleteCopy(copyInfo)
              pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, copyInfo.systemId, copyInfo.copyNumber,
                TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                secondaryCopyInfo.dataVersion)
            case None =>
              val secCopyId = pgu.secondaryDatasetMapWriter.allocateCopyId()
              pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, secCopyId,
                secondaryCopyInfo.copyNumber,
                TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                secondaryCopyInfo.dataVersion)
          }
        }.getOrElse(throw new Exception(
          s"""Cannot find existing dataset info.
             |  You may manually delete dataset_internal_name_map record and start fresh
             |  ${secondaryDatasetInfo.internalName} ${dsId}
           """.stripMargin))
    }

    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(truthCopyInfo)

    newSchema.values.foreach { col =>
      ColumnCreatedHandler(pgu, truthCopyInfo, col)
    }

    val truthSchema: ColumnIdMap[ColumnInfo[SoQLType]] = pgu.datasetMapReader.schema(truthCopyInfo)
    val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
    sLoader.deoptimize(truthSchema.values)
    val loader = pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "test-user"))

    val sidColumnInfo = newSchema.values.find(_.isSystemPrimaryKey).get

    for { iter <- rows } {
      // TODO: divide rows based on data size instead of number of rows.
      iter.grouped(resyncBatchSize).foreach { bi =>
        for { row: ColumnIdMap[SoQLValue] <- bi } {
          logger.trace("adding row: {}", row)
          val rowId = pgu.commonSupport.typeContext.makeSystemIdFromValue(row.get(sidColumnInfo.systemId).get)
          loader.insert(rowId, row)
        }
        loader.flush()
      }
    }
    sLoader.optimize(truthSchema.values)

    if (truthCopyInfo.lifecycleStage == TruthLifecycleStage.Unpublished &&
      secondaryCopyInfo.lifecycleStage == LifecycleStage.Published) {
      pgu.datasetMapWriter.publish(truthCopyInfo)
    }

    if (truthCopyInfo.dataVersion != secondaryCopyInfo.dataVersion) {
      pgu.datasetMapWriter.updateDataVersion(truthCopyInfo, secondaryCopyInfo.dataVersion)
    }
    pgu.datasetMapWriter.updateLastModified(truthCopyInfo, secondaryCopyInfo.lastModified)

    val postUpdateTruthCopyInfo = pgu.datasetMapReader.latest(truthCopyInfo.datasetInfo)
    // re-create rollup metadata
    for { rollup <- rollups } RollupCreatedOrUpdatedHandler(pgu, postUpdateTruthCopyInfo, rollup)
    // re-create rollup tables
    updateRollups(postUpdateTruthCopyInfo)
  }

  def doDropCopy(isLatestCopy: Boolean): Unit = {
    // If this is the latest copy we want make sure this copy is in the `copy_map`.
    // (Because secondary-watcher will write back to the secondary manifest that we
    // are up to this copy's data-version.)
    // If this is the case, resync(.) will have already been called on a (un)published
    // copy and appropriate metadata should exist for this dataset.
    // Otherwise, we want to drop this copy entirely.
    pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName) match {
      case None =>
        // Don't bother dropping a copy
        if (isLatestCopy) {
          // Well this is weird, but only going to log for now...
          logger.error("No dataset found for {} when dropping the latest copy {} for data version {}?",
            secondaryDatasetInfo.internalName, secondaryCopyInfo.copyNumber.toString, secondaryCopyInfo.dataVersion.toString)
        }
      case Some(dsId) =>
        pgu.datasetMapReader.datasetInfo(dsId).map { truthDatasetInfo =>
          pgu.datasetMapWriter.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber) match {
            case Some(copyInfo) =>
              // drop the copy
              logger.info(
                "dropping copy {} {}",
                truthDatasetInfo.systemId.toString,
                secondaryCopyInfo.copyNumber.toString
              )
              val rm = new RollupManager(pgu, copyInfo)
              rm.dropRollups(immediate = true) // drop all rollup tables
              pgu.secondaryDatasetMapWriter.deleteCopy(copyInfo)
              if (isLatestCopy) {
                // create the `copy_map` entry
                pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, copyInfo.systemId, copyInfo.copyNumber,
                  TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                  secondaryCopyInfo.dataVersion)
              }
            case None =>
              // no copy to drop
              if (isLatestCopy) {
                // create the `copy_map` entry
                val secCopyId = pgu.secondaryDatasetMapWriter.allocateCopyId()
                pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo, secCopyId,
                  secondaryCopyInfo.copyNumber,
                  TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
                  secondaryCopyInfo.dataVersion)
              }
          }
        }
    }
  }

}
