package com.socrata.pg.store

import java.io.Closeable
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.socrata.curator.CuratorFromConfig
import com.socrata.pg.BuildInfo
import com.mchange.v2.c3p0.DataSources
import com.rojoma.json.v3.ast.JObject
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo, CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage}
import com.socrata.datacoordinator.truth.universe.sql.{C3P0WrappedPostgresCopyIn, PostgresCopyIn}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.SecondaryBase
import com.socrata.pg.config.StoreConfig
import com.socrata.pg.store.events.{ColumnCreatedHandler, WorkingCopyPublishedHandler, _}
import com.socrata.pg.store.index._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.postgresql.ds.PGSimpleDataSource

object PGSecondary {
  private val logger = Logger[PGSecondary]
}

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] with SecondaryBase {
  import PGSecondary.logger

  logger.info(BuildInfo.toJson)

  val storeConfig = new StoreConfig(config, "")
  override val dsConfig =  storeConfig.database

  private val dsInfo = dataSourceFromConfig(dsConfig)
  private val finished = new CountDownLatch(1)
  private val tableDropper = startTableDropper()
  private val resyncBatchSize = storeConfig.resyncBatchSize
  private val tableDropTimeoutSeconds: Long = 60
  private val curator = storeConfig.curatorConfig.map { cc =>
    val client = CuratorFromConfig.unmanaged(cc)
    client.start()
    client
  }

  val rowsChangedPreviewConfig =
    new RowsChangedPreviewConfig.Clojure(storeConfig.transparentCopyFunction.getOrElse(RowsChangedPreviewConfig.default))
  private val rowsChangedPreviewHandler = new RowsChangedPreviewHandler(rowsChangedPreviewConfig)

  val postgresUniverseCommon = new PostgresUniverseCommon(TablespaceFunction(storeConfig.tablespace), dsInfo.copyIn)

  private val secondaryMetrics = storeConfig.secondaryMetrics

  // Called when this process is shutting down (or being killed)
  def shutdown(): Unit = {
    logger.info("shutting down {} {} ...", dsConfig.host, dsConfig.database)
    finished.countDown()
    tableDropper.join()
    dsInfo.close()
    rowsChangedPreviewConfig match {
      case closable: Closeable => closable.close()
      case _ => // ok
    }
    curator.foreach(_.close())
    logger.info("shut down {} {}", dsConfig.host, dsConfig.database)
  }

  // This is the last event we will every receive for a given dataset. Receiving this event means
  // that all data related to that dataset can/should be destroyed
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie): Unit = {
    // last thing you will get for a dataset.
    logger.debug(s"dropDataset '${datasetInternalName}' (cookie : ${cookie}) ")
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) =>
          val datasetId = copyInfo.datasetInfo.systemId
          for { copy <- pgu.datasetMapReader.allCopies(copyInfo.datasetInfo) } {
            val rm = new RollupManager(pgu, copy)
            rm.dropRollups(immediate = false)
          }
          pgu.datasetDropper.dropDataset(datasetId)
          pgu.commit()
        case None =>
          // TODO: Clean possible orphaned dataset internal name map
          logger.warn("drop dataset called but cannot find a copy {}", datasetInternalName)
      }
    }
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      doCurrentVersion(pgu, datasetInternalName, cookie)
    }
  }

  // Every set of changes increments the version number, so a given copy (number) may have
  // multiple versions over the course of it's life
  def doCurrentVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       datasetInternalName: String,
                       cookie: Secondary.Cookie): Long = {
    // every set of changes to a copy increments the version number
    // What happens when this is wrong? Almost certainly should turn into a resync
    logger.debug(s"currentVersion '${datasetInternalName}', (cookie: ${cookie})")

    truthCopyInfo(pgu, datasetInternalName) match {
      case Some(copy) => copy.dataVersion
      case None =>
        logger.warn("current version called but cannot find a copy {}", datasetInternalName)
        0
    }
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(dsInfo, None) { pgu =>
      doCurrentCopyNumber(pgu, datasetInternalName, cookie)
    }
  }

  // Current copy number is incremented every time a copy is made within the data coordinator
  // Publishing or snapshotting does not increment the copy number
  // The datasetmap contains both the current version number and current copy number and should be consulted to
  // determine the copy number value to return in this method. The datasetmap resides in the metadata db and can
  // be looked up by datasetInternalName
  protected[store] def doCurrentCopyNumber(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                           datasetInternalName: String,
                                           cookie: Secondary.Cookie): Long = {
    // Always incremented a working copy is made in the datacoordinator
    // the current copy number should always come out of the resync call or WorkingCopyCreatedEvent
    // still need the mapping from ds internal name to the copy number
    //
    // if we do not do working copies; we *should* receive a resync event instead of a publish event
    //
    // What happens if this is wrong? almost certainly it would turn into a resync
    logger.debug(s"currentCopyNumber '${datasetInternalName}' (cookie: ${cookie})")

    truthCopyInfo(pgu, datasetInternalName) match {
      case Some(copy) => copy.copyNumber
      case None =>
        logger.warn("current copy number called but cannot find a copy {}", datasetInternalName)
        0
    }
  }

  // Is only ever called as part of a resync.
  def dropCopy(datasetInfo: DatasetInfo,
               secondaryCopyInfo: SecondaryCopyInfo,
               cookie: Secondary.Cookie, isLatestCopy: Boolean): Secondary.Cookie = {
    logger.info("droping copy (datasetInfo: {}, secondaryCopyInfo: {}, cookie: {})",
                datasetInfo, secondaryCopyInfo, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      doDropCopy(pgu, datasetInfo, secondaryCopyInfo, isLatestCopy)
      pgu.commit()
    }
    cookie
  }

  def doDropCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: DatasetInfo,
                 secondaryCopyInfo: SecondaryCopyInfo, isLatestCopy: Boolean): Unit = {
    // If this is the latest copy we want make sure this copy is in the `copy_map`.
    // (Because secondary-watcher will write back to the secondary manifest that we
    // are up to this copy's data-version.)
    // If this is the case, resync(.) will have already been called on a (un)published
    // copy and appropriate metadata should exist for this dataset.
    // Otherwise, we want to drop this copy entirely.
    pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInfo.internalName) match {
      case None =>
        // Don't bother dropping a copy
        if (isLatestCopy) {
          // Well this is weird, but only going to log for now...
          logger.error("No dataset found for {} when dropping the latest copy {} for data version {}?",
            datasetInfo.internalName, secondaryCopyInfo.copyNumber.toString, secondaryCopyInfo.dataVersion.toString)
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

  def version(datasetInfo: DatasetInfo,
              initialDataVersion: Long,
              finalDataVersion: Long,
              cookie: Secondary.Cookie,
              events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
     val cookieOut = doVersion(pgu, datasetInfo, initialDataVersion, finalDataVersion, cookie, events)
     pgu.commit()
     cookieOut
    }
  }


  // The main method by which data will be sent to this API.
  // workingCopyCreated event is the (first) event by which this method will be called
  // "You always update the latest copy through the version method"
  // A separate event will be passed to this method for actually copying the data
  // If working copy already exists and we receive a workingCopyCreated event is received, then a resync
  // event/exception should fire.
  // Publishing a working copy promotes that working copy to a published copy.
  // There should no longer be a working copy after publishing
  // scalastyle:ignore method.length cyclomatic.complexity
  def doVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                        secondaryDatasetInfo: DatasetInfo,
                        newDataVersion: Long,
                        finalDataVersion: Long,
                        cookie: Secondary.Cookie,
                        events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    // How do we get the copyInfo? dataset_map
    //  - One of the events that comes through here will be working copy created, it must be the first if it does,
    //    separate event for actually copying the data
    //  - Always update the latest copy through version if no working copy created event is passed in
    //  - If you have a working copy and you get a working copy created event; resync
    //  - If you don't have a working copy and you get a publish event; resync
    //  - Unpublished => Working Copy

    // rowVersion is given through the event
    // newDataVersion is the version which corresponds to the set of events which we are given and
    //   corresponds with the currentVersion
    //     - ignore this if the newDataVersion <= currentVersion
    //     - stored in copy_map
    logger.debug("version (secondaryDatasetInfo: %s, newDataVersion: %s, cookie: %s, events: %s)".format(
      secondaryDatasetInfo, newDataVersion, cookie, events
      ))
    // if we have a WorkingCopyCreated event, it is supposed to be the first event in the version,
    // and the only WorkingCopyCreated event in the version.
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    val hasWccEvents = wccEvents.hasNext

    if (wccEvents.hasNext) {
      val e = wccEvents.next()
      logger.debug("got working copy event: {}", e)

      val existingDataset =
        for {
          datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName)
          datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
        } yield {
          datasetInfo
        }

      e match {
        case WorkingCopyCreated(copyInfo) =>
          val theCopy = existingDataset.flatMap { dsInfo =>
            val allCopies = pgu.datasetMapReader.allCopies(dsInfo)
            // Either this copy or some newer copy
            allCopies.find(existingCopyInfo => existingCopyInfo.copyNumber >= copyInfo.copyNumber)
          }
          if (theCopy.isDefined) {
            logger.info("dataset {} working copy {} already existed, resync",
              secondaryDatasetInfo.internalName, theCopy.get.copyNumber.toString)
            throw new ResyncSecondaryException("Dataset already exists")
          } else {
            WorkingCopyCreatedHandler(pgu,
              pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName),
              secondaryDatasetInfo, copyInfo)
          }
        case _ => throw new UnsupportedOperationException("Unexpected operation")
      }
    }

    if (wccEvents.hasNext) throw new UnsupportedOperationException(
        s"Got ${wccEvents.size + 1} leading WorkingCopyCreated events, only support one in a version"
      )

    // now that we have working copy creation out of the way, we can load our
    // secondaryCopyInfo with assurance that it is there unless we are out of sync
    val dsId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
      throw new ResyncSecondaryException(
        s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}"
      )
    )

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(dsId).get
    val initialTruthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
    val previouslyPublishedTruthCopyInfo = pgu.datasetMapReader.published(truthDatasetInfo)

    // If there's more than one version's difference between the last known copy in truth
    // and the last known copy in the secondary, force a resync.
    // TODO : It seems weird to resync just because there's a version gap,
    // but the PG secondary code has always worked this way, and we've never
    // verified that playing back multiple versions in one go actually works in
    // every case. At some point, we should take the time to verify this and
    // revisit whether we actually want this resync logic to exist.
    val allCopiesInOrder = pgu.datasetMapReader.allCopies(truthDatasetInfo)
    val dvExpect = allCopiesInOrder.maxBy(_.dataVersion).dataVersion + 1
    if (newDataVersion != dvExpect) {
      throw new ResyncSecondaryException(
       s"Current version ${initialTruthCopyInfo.dataVersion}, next version ${newDataVersion} but should be ${dvExpect}")
    }

    if (!hasWccEvents) {
      // If there are no writes that have happened here yet,
      // commit to release unnecessary locks on system tables like dataset_internal_name_map, dataset_map.
      // rows updates still cause locks on copy_map, column_map though.
      pgu.commit()
    }

    // no rebuild index and no rows loader.
    val startCtx = (false, initialTruthCopyInfo, Option.empty[SqlPrevettedLoader[SoQLType, SoQLValue]])
    val (rebuildIndex, finalTruthCopyInfo, _) = remainingEvents.foldLeft(startCtx) { (ctx, e) =>
      logger.debug("got event: {}", e)
      val (rebuildIndex, truthCopyInfo, dataLoader) = ctx
      e match {
        case Truncated =>
          TruncateHandler(pgu, truthCopyInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case ColumnCreated(secondaryColInfo) =>
          ColumnCreatedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, truthCopyInfo, None)
        case ColumnRemoved(secondaryColInfo) =>
          ColumnRemovedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, truthCopyInfo, None)
        case FieldNameUpdated(secondaryColInfo) =>
          FieldNameUpdatedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex,  truthCopyInfo, None)
        case RowIdentifierSet(info) =>  // no-op
          (rebuildIndex, truthCopyInfo, dataLoader)
        case RowIdentifierCleared(info) =>  // no-op
          (rebuildIndex, truthCopyInfo, dataLoader)
        case SystemRowIdentifierChanged(secondaryColInfo) =>
          SystemRowIdentifierChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case VersionColumnChanged(secondaryColInfo) =>
          VersionColumnChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        // TODO when dealing with dropped, figure out what version we have to updated afterwards and how that works...
        // maybe workingcopydropped is guaranteed to be the last event in a batch, and then we can skip updating the
        // version anywhere... ?
        case WorkingCopyDropped =>
          WorkingCopyDroppedHandler(pgu, truthDatasetInfo)
          (rebuildIndex, truthCopyInfo, None)
        case DataCopied =>
          DataCopiedHandler(pgu, truthDatasetInfo, truthCopyInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case SnapshotDropped(info) =>
          logger.info("drop snapshot system id - {}, copy number - {}",
            info.systemId.toString(), info.copyNumber.toString)
          pgu.datasetMapReader.copyNumber(truthDatasetInfo, info.copyNumber) match {
            case Some(ci) =>
              CopyDroppedHandler(pgu, ci)
            case None =>
              throw new Exception("cannot find copy to drop")
          }
          (rebuildIndex, truthCopyInfo, None)
        case WorkingCopyPublished =>
          WorkingCopyPublishedHandler(pgu, truthCopyInfo)
          (true, truthCopyInfo, dataLoader)
        case RowDataUpdated(ops) =>
          val loader = dataLoader.getOrElse(prevettedLoader(pgu, truthCopyInfo))
          RowDataUpdatedHandler(loader, ops)
          (rebuildIndex, truthCopyInfo, Some(loader))
        case LastModifiedChanged(lastModified) =>
          pgu.datasetMapWriter.updateLastModified(truthCopyInfo, lastModified)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case RollupCreatedOrUpdated(rollupInfo) =>
          RollupCreatedOrUpdatedHandler(pgu, truthCopyInfo, rollupInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case RollupDropped(rollupInfo) =>
          RollupDroppedHandler(pgu, truthCopyInfo, rollupInfo)
          (rebuildIndex, truthCopyInfo, dataLoader)
        case ComputationStrategyCreated(_) => // no op
          (rebuildIndex, truthCopyInfo, dataLoader)
        case ComputationStrategyRemoved(_) => // no op
          (rebuildIndex, truthCopyInfo, dataLoader)
        case RowsChangedPreview(inserted, updated, deleted, truncated) =>
          rowsChangedPreviewHandler(pgu, truthDatasetInfo, truthCopyInfo, truncated, inserted, updated, deleted) match {
            case Some(nci) =>
              val isPublished = nci.lifecycleStage == TruthLifecycleStage.Published
              (isPublished, nci, None)
            case None =>
              (rebuildIndex, truthCopyInfo, dataLoader)
          }
        case SecondaryReindex =>
          (true, truthCopyInfo, dataLoader)
        case IndexDirectiveCreatedOrUpdated(secondaryColInfo, directive) =>
          IndexDirectiveCreatedOrUpdatedHandler(pgu, truthColumnInfo(pgu, truthCopyInfo, secondaryColInfo), directive)
          (true, truthCopyInfo, dataLoader)
        case IndexDirectiveDropped(secondaryColInfo) =>
          IndexDirectiveDroppedHandler(pgu, truthColumnInfo(pgu, truthCopyInfo, secondaryColInfo))
          (true, truthCopyInfo, dataLoader)
        case otherOps: Event[SoQLType,SoQLValue] =>
          throw new UnsupportedOperationException(s"Unexpected operation $otherOps")
      }
    }

    pgu.datasetMapWriter.updateDataVersion(finalTruthCopyInfo, finalDataVersion)

    def maybeLogTime[T](label: String)(f: => T): T = {
      val startedAt =
        if(finalTruthCopyInfo.tableModifier != initialTruthCopyInfo.tableModifier) {
          Some(System.nanoTime())
        } else {
          None
        }
      val result = f
      startedAt.foreach { start =>
        val durationMS = (System.nanoTime() - start)/1000000
        logger.info(s"$label after making a copy took ${durationMS / 1000.0}s")
      }
      result
    }

    if (rebuildIndex) {
      maybeLogTime("Rebuilding indexes") {
        val schema = pgu.datasetMapReader.schema(finalTruthCopyInfo)
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        sLoader.createIndexes(schema.values)
        sLoader.createFullTextSearchIndex(schema.values)
        pgu.analyzer.analyze(finalTruthCopyInfo)
      }
    }

    val postUpdateTruthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
    // Rollups do not materialize if stage is unpublished.
    if (RollupManager.shouldMaterializeRollups(postUpdateTruthCopyInfo.lifecycleStage)) {
      maybeLogTime("Refreshing rollups") {
        updateRollups(pgu,
                      previouslyPublishedTruthCopyInfo,
                      postUpdateTruthCopyInfo)
      }
    }

    cookie
  }

  // This is an expensive operation in that it is both time consuming as well as locking the data source for the
  // duration of the resync event. The resync event can come from either the DC, or originate from a
  // ResyncSecondaryException being thrown.
  // Incoming rows have their own ids already provided at this point
  // Need to record some state somewhere so that readers can know that a resync is underway
  // Backup code (Receiver class) touches this method as well via the receiveResync method
  // SoQL ID is only ever used for row ID
  def resync(datasetInfo: DatasetInfo,
             secondaryCopyInfo: SecondaryCopyInfo,
             schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
             cookie: Secondary.Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo],
             indexDirectives: Seq[com.socrata.datacoordinator.truth.metadata.IndexDirective[SoQLType]],
             isLatestLivingCopy: Boolean): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.info("resync (datasetInfo: {}, secondaryCopyInfo: {}, schema: {}, cookie: {})",
      datasetInfo, secondaryCopyInfo, schema, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      val cookieOut = doResync(pgu, datasetInfo, secondaryCopyInfo, schema, cookie, rows, rollups, indexDirectives)
      pgu.commit()
      cookieOut
    }
  }

  // scalastyle:ignore method.length cyclomatic.complexity
  def doResync(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
               secondaryDatasetInfo: DatasetInfo,
               secondaryCopyInfo: SecondaryCopyInfo,
               newSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
               cookie: Secondary.Cookie,
               rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
               rollups: Seq[RollupInfo],
               indexDirectives: Seq[com.socrata.datacoordinator.truth.metadata.IndexDirective[SoQLType]]): Secondary.Cookie =
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
        val newDatasetInfo = pgu.datasetMapWriter.unsafeCreateDatasetAllocatingSystemId(secondaryDatasetInfo.localeName, secondaryDatasetInfo.obfuscationKey, secondaryDatasetInfo.resourceName)
        val newDatasetId = newDatasetInfo.systemId
        logger.info("new secondary dataset {} {}", secondaryDatasetInfo.internalName, newDatasetId.toString())
        pgu.secondaryDatasetMapWriter.createInternalNameMapping(secondaryDatasetInfo.internalName, newDatasetId)
        val secCopyId = pgu.secondaryDatasetMapWriter.allocateCopyId()
        pgu.datasetMapWriter.unsafeCreateCopy(newDatasetInfo, secCopyId,
          secondaryCopyInfo.copyNumber,
          TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
          secondaryCopyInfo.dataVersion)
      case Some(dsId) =>
        // Find the very top record - dataset_internal_name_map.
        // Delete and recreate the copy with the same dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest
        //      SET broken_at = null,
        //          latest_secondary_data_version = 0,
        //          latest_secondary_lifecycle_stage = 'Unpublished'
        //    WHERE dataset_system_id = ? -- 20
        // Disable dataset for soql reads during resync to avoid lock issue which
        // leads to backing up of connections waiting for lock.
        pgu.datasetMapWriter.disableDataset(dsId)
        pgu.commit()
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
    truthSchema.values.find(_.isSystemPrimaryKey).foreach { pkCol =>
      sLoader.makeSystemPrimaryKey(pkCol)
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

    // re-create index directives
    for { idx <- indexDirectives } {
      IndexDirectiveCreatedOrUpdatedHandler(pgu, idx.columnInfo, idx.directive)
    }

    // re-create rollup tables.  We don't need to pass in an old truth
    // because we just dropped any existing rollups above.
    updateRollups(pgu, None, postUpdateTruthCopyInfo)

    pgu.datasetMapWriter.enableDataset(truthCopyInfo.datasetInfo.systemId) // re-enable soql reads
    cookie
  }

  private def truthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            datasetInternalName: String): Option[TruthCopyInfo] = {
    for {
      datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInternalName)
      truthDatasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      pgu.datasetMapReader.latest(truthDatasetInfo)
    }
  }

  private def truthColumnInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                              truthCopyInfo: TruthCopyInfo,
                              secColInfo: SecondaryColumnInfo[SoQLType]): ColumnInfo[SoQLType] = {

      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
      val truthCopyContext = new DatasetCopyContext[SoQLType](truthCopyInfo, pgu.datasetMapReader.schema(truthCopyInfo))
      truthCopyContext.thaw().columnInfo(secColInfo.systemId)
  }

  private def updateRollups(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            originalCopyInfo: Option[TruthCopyInfo],
                            copyInfo: TruthCopyInfo) = {
    val rm = new RollupManager(pgu, copyInfo)
    rm.dropRollups(immediate = true)
    pgu.datasetMapReader.rollups(copyInfo).foreach { ri =>
      rm.updateRollup(ri, originalCopyInfo, copyInfo.dataVersion)
    }
  }

  private def prevettedLoader(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo) = {
      val truthSchema = pgu.datasetMapReader.schema(truthCopyInfo)
      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
      pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "user"))
  }

  private def startTableDropper() = {
    val tableDropper = new Thread() {
      setName(s"pg-sec-table-dropper-${storeConfig.database.host}")
      override def run(): Unit = {
        while (!finished.await(tableDropTimeoutSeconds, TimeUnit.SECONDS)) {
          try {
            withPgu(dsInfo, None) { pgu =>
              while(finished.getCount > 0 && pgu.tableCleanup.cleanupPendingDrops()) {
                pgu.commit()
              }
            }
          } catch {
            case e: Exception => logger.error("Unexpected error while dropping secondary tables", e)
          }
        }
      }
    }
    tableDropper.start()
    tableDropper
  }

  private def dataSourceFromConfig(config: DataSourceConfig): DSInfo with Closeable = {
    val dataSource = new PGSimpleDataSource
    dataSource.setServerName(config.host)
    dataSource.setPortNumber(config.port)
    dataSource.setDatabaseName(config.database)
    dataSource.setUser(config.username)
    dataSource.setPassword(config.password)
    dataSource.setApplicationName(config.applicationName)
    config.poolOptions match {
      case Some(poolOptions) =>
        val overrideProps = C3P0Propertizer("", poolOptions)
        val pooled = DataSources.pooledDataSource(dataSource, null, overrideProps) // scalastyle:ignore null
        new DSInfo(pooled, C3P0WrappedPostgresCopyIn) with Closeable {
          def close(): Unit = DataSources.destroy(pooled)
        }
      case None =>
        new DSInfo(dataSource, PostgresCopyIn) with Closeable {
          def close(): Unit = {}
        }
    }
  }

  override def metric(datasetInternalName: String, cookie: Cookie): Option[SecondaryMetric] = {
    if (secondaryMetrics.enabled) {
      logger.debug(s"metric '$datasetInternalName' (cookie: $cookie)")
      withPgu(dsInfo, None) { pgu =>
        val result = doMetric(pgu, datasetInternalName)
        pgu.commit()

        result
      }
    } else {
      logger.debug("skipping metric since dataset secondary metrics are not enabled")
      None
    }
  }

  private def doMetric(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       datasetInternalName: String): Option[SecondaryMetric] = {
    pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInternalName).flatMap { datasetId =>
      pgu.secondaryMetrics.dataset(datasetId)
    }
  }
}
