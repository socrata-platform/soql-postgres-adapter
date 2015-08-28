package com.socrata.pg.store

import com.mchange.v2.c3p0.DataSources
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.Managed
import com.rojoma.simplearm.util._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage, ColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.secondary.VersionColumnChanged
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.RowIdentifierSet
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.RowIdentifierCleared
import com.socrata.pg.config.StoreConfig
import com.socrata.pg.store.events._
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.SnapshotDropped
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.pg.store.events.ColumnCreatedHandler
import com.socrata.datacoordinator.secondary.RowDataUpdated
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.datacoordinator.secondary.ColumnRemoved
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCopyIn, C3P0WrappedPostgresCopyIn}
import com.socrata.pg.store.events.WorkingCopyPublishedHandler
import com.socrata.pg.{Version, SecondaryBase}
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import java.io.Closeable
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.postgresql.ds.PGSimpleDataSource


/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] with SecondaryBase with Logging {

  val storeConfig = new StoreConfig(config, "")

  override val dsConfig =  storeConfig.database

  logger.info(JsonUtil.renderJson(Version("store-pg")))

  private val dsInfo = dataSourceFromConfig(dsConfig)

  val postgresUniverseCommon = new PostgresUniverseCommon(TablespaceFunction(storeConfig.tablespace), dsInfo.copyIn)

  private val finished = new CountDownLatch(1)

  private val resyncBatchSize = storeConfig.resyncBatchSize

  // Called when this process is shutting down (or being killed)
  def shutdown() {
    logger.info("shutting down {} {} ...", dsConfig.host, dsConfig.database)
    finished.countDown()
    dsInfo.close()
    logger.info("shut down {} {}", dsConfig.host, dsConfig.database)
  }

  // Return true to get all the events from the stream of updates from the data-coordinator
  // Returning false here means that instead of a stream of updates from the DC, we will receive
  // the resync event instead.
  def wantsWorkingCopies: Boolean = {
    logger.debug("wantsWorkingCopies returning true")
    true
  }

  // This is the last event we will every receive for a given dataset. Receiving this event means
  // that all data related to that dataset can/should be destroyed
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) {
    // last thing you will get for a dataset.
    logger.debug(s"dropDataset '${datasetInternalName}' (cookie : ${cookie}) ")
    withPgu(dsInfo, None) { pgu =>
      truthCopyInfo(pgu, datasetInternalName) match {
        case Some(copyInfo) =>
          val datasetId = copyInfo.datasetInfo.systemId
          for (copy <- pgu.datasetMapReader.allCopies(copyInfo.datasetInfo)) {
            val rm = new RollupManager(pgu, copy)
            rm.dropRollups(immediate = true)
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
      _currentVersion(pgu, datasetInternalName, cookie)
    }
  }

  // Every set of changes increments the version number, so a given copy (number) may have
  // multiple versions over the course of it's life
  def _currentVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String, cookie: Secondary.Cookie): Long = {
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
      _currentCopyNumber(pgu, datasetInternalName, cookie)
    }
  }

  // Current copy number is incremented every time a copy is made within the data coordinator
  // Publishing or snapshotting does not increment the copy number
  // The datasetmap contains both the current version number and current copy number and should be consulted to determine
  // the copy number value to return in this method. The datasetmap resides in the metadata db and can be looked up by datasetInternalName
  protected[store] def _currentCopyNumber(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String, cookie: Secondary.Cookie): Long = {
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

  // Currently there are zero-or-more snapshots, which are what you get when you publish a working copy when there is
  // an existing published working copy of that same dataset.
  // To NO-OP this API, return an empty set.
  @deprecated("Not supporting snapshots beyond bare minimum required to function", since = "forever")
  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie): Set[Long] = {
    // if we a publish through version(); a snapshot "could" be created
    logger.debug(s"snapshots '${datasetInternalName}' (cookie: ${cookie}})")
    Set()
  }

  // Is only ever called as part of a resync.
  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie): Secondary.Cookie = {
    // TODO: dropCopy
    logger.warn(s"TODO: dropCopy '${datasetInternalName}' (cookie: ${cookie}})")
    cookie
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
     val cookieOut = _version(pgu, datasetInfo, dataVersion, cookie, events)
     pgu.commit()
     cookieOut
    }
  }

  // The main method by which data will be sent to this API.
  // workingCopyCreated event is the (first) event by which this method will be called
  // "You always update the latest copy through the version method"
  // A separate event will be passed to this method for actually copying the data
  // If working copy already exists and we receive a workingCopyCreated event is received, then a resync event/exception should fire
  // Publishing a working copy promotes that working copy to a published copy. There should no longer be a working copy after publishing
  def _version(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], secondaryDatasetInfo: DatasetInfo, newDataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    // How do we get the copyInfo? dataset_map
    //  - One of the events that comes through here will be working copy created; it must be the first if it does; separate event for actually copying
    //    the data
    //  - Always update the latest copy through version if no working copy created event is passed in
    //  - If you have a working copy and you get a working copy created event; resync
    //  - If you don't have a working copy and you get a publish event; resync
    //  - Unpublished => Working Copy

    // rowVersion is given through the event
    // newDataVersion is the version which corresponds to the set of events which we are given; corresponds with the currentVersion
    //     - ignore this if the newDataVersion <= currentVersion
    //     - stored in copy_map
    logger.debug(s"version (secondaryDatasetInfo: ${secondaryDatasetInfo}}, newDataVersion: ${newDataVersion}}, cookie: ${cookie}, events: ${events})")
    // if we have a WorkingCopyCreated event, it is supposed to be the first event in the version,
    // and the only WorkingCopyCreated event in the version.
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

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
              allCopies.find(existingCopyInfo => existingCopyInfo.copyNumber == copyInfo.copyNumber)
          }
          if (theCopy.isDefined) {
            logger.info("dataset {} working copy {} already existed, resync", secondaryDatasetInfo.internalName, theCopy.get.copyNumber.toString)
            throw new ResyncSecondaryException("Dataset already exists")
          } else {
            val truthDatasetInfo = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName)
            WorkingCopyCreatedHandler(pgu, truthDatasetInfo, secondaryDatasetInfo, copyInfo)
          }
        case otherOps =>
          throw new UnsupportedOperationException("Unexpected operation")
      }
    }

    if (wccEvents.hasNext) {
      throw new UnsupportedOperationException(s"Got ${wccEvents.size+1} leading WorkingCopyCreated events, only support one in a version")
    }

    // now that we have working copy creation out of the way, we can load our
    // secondaryCopyInfo with assurance that it is there unless we are out of sync
    val datasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse(
      throw new ResyncSecondaryException(s"Couldn't find mapping for datasetInternalName ${secondaryDatasetInfo.internalName}")
    )

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
    val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

    if (truthCopyInfo.dataVersion + 1 != newDataVersion) {
      throw new ResyncSecondaryException(s"Current version ${truthCopyInfo.dataVersion}, next version ${newDataVersion} but should be ${truthCopyInfo.dataVersion+1}")
    }

    // no rebuild index, no refresh rollup and no rows loader.
    val startCtx = (false, false, Option.empty[SqlPrevettedLoader[SoQLType, SoQLValue]])
    val (rebuildIndex, refreshRollup, _) = remainingEvents.foldLeft(startCtx) { (ctx, e) =>
      logger.debug("got event: {}", e)
      val (rebuildIndex, refreshRollup, dataLoader) = ctx
      e match {
        case Truncated =>
          TruncateHandler(pgu, truthCopyInfo)
          (rebuildIndex, true, dataLoader)
        case ColumnCreated(secondaryColInfo) =>
          ColumnCreatedHandler(pgu, truthCopyInfo, secondaryColInfo)
          val shouldRefreshRollup = refreshRollup || (truthCopyInfo.lifecycleStage == TruthLifecycleStage.Published)
          (rebuildIndex, true, None)
        case ColumnRemoved(secondaryColInfo) =>
          ColumnRemovedHandler(pgu, truthCopyInfo, secondaryColInfo)
          val shouldRefreshRollup = refreshRollup || (truthCopyInfo.lifecycleStage == TruthLifecycleStage.Published)
          (rebuildIndex, true, None)
        case RowIdentifierSet(info) =>  // no-op
          (rebuildIndex, true, dataLoader)
        case RowIdentifierCleared(info) =>  // no-op
          (rebuildIndex, true, dataLoader)
        case SystemRowIdentifierChanged(secondaryColInfo) =>
          SystemRowIdentifierChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, true, dataLoader)
        case VersionColumnChanged(secondaryColInfo) =>
          VersionColumnChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, true, dataLoader)
        // TODO when dealing with dropped, figure out what version we have to updated afterwards and how that works...
        // maybe workingcopydropped is guaranteed to be the last event in a batch, and then we can skip updating the
        // version anywhere... ?
        case WorkingCopyDropped =>
          WorkingCopyDroppedHandler(pgu, truthDatasetInfo)
          (rebuildIndex, false, None)
        case DataCopied =>
          DataCopiedHandler(pgu, truthDatasetInfo, truthCopyInfo)
          (rebuildIndex, true, dataLoader)
        case SnapshotDropped(info) =>
          logger.info("drop snapshot system id - {}, copy number - {}", info.systemId.toString, info.copyNumber.toString)
          pgu.datasetMapReader.copyNumber(truthDatasetInfo, info.copyNumber) match {
            case Some(ci) =>
              CopyDroppedHandler(pgu, ci)
            case None =>
              throw new Exception("cannot find copy to drop")
          }
          (rebuildIndex, refreshRollup, None)
        case WorkingCopyPublished =>
          WorkingCopyPublishedHandler(pgu, truthCopyInfo)
          (true, true, dataLoader)
        case RowDataUpdated(ops) =>
          val loader = dataLoader.getOrElse(prevettedLoader(pgu, truthCopyInfo))
          RowDataUpdatedHandler(loader, ops)
          (rebuildIndex, true, Some(loader))
        case LastModifiedChanged(lastModified) =>
          pgu.datasetMapWriter.updateLastModified(truthCopyInfo, lastModified)
          (rebuildIndex, true, dataLoader)
        case RollupCreatedOrUpdated(rollupInfo) =>
          RollupCreatedOrUpdatedHandler(pgu, truthCopyInfo, rollupInfo)
          (rebuildIndex, true, dataLoader)
        case RollupDropped(rollupInfo) =>
          RollupDroppedHandler(pgu, truthCopyInfo, rollupInfo)
          (rebuildIndex, true, dataLoader)
        case otherOps =>
          throw new UnsupportedOperationException(s"Unexpected operation $otherOps")
      }
    }

    pgu.datasetMapWriter.updateDataVersion(truthCopyInfo, newDataVersion)

    if (rebuildIndex) {
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
      sLoader.createIndexes(schema.values)
      sLoader.createFullTextSearchIndex(schema.values)
    }

    // Rollups do not materialize if stage is unpublished.
    if (refreshRollup) {
      val postUpdateTruthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
      updateRollups(pgu, postUpdateTruthCopyInfo, true)
    }

    cookie
  }

  // This is an expensive operation in that it is both time consuming as well as locking the data source for the duration
  // of the resync event. The resync event can come from either the DC, or originate from a ResyncSecondaryException being thrown
  // Incoming rows have their own ids already provided at this point
  // Need to record some state somewhere so that readers can know that a resync is underway
  // Backup code (Receiver class) touches this method as well via the receiveResync method
  // SoQL ID is only ever used for row ID
  def resync(datasetInfo: DatasetInfo, secondaryCopyInfo: SecondaryCopyInfo, schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]], cookie: Secondary.Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo]): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.info("resync (datasetInfo: {}, secondaryCopyInfo: {}, schema: {}, cookie: {})", datasetInfo, secondaryCopyInfo, schema, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      val cookieOut = _resync(pgu, datasetInfo, secondaryCopyInfo, schema, cookie, rows, rollups)
      pgu.commit()
      cookieOut
    }
  }

  def _resync(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
              secondaryDatasetInfo: DatasetInfo,
              secondaryCopyInfo: SecondaryCopyInfo,
              newSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
              cookie: Secondary.Cookie,
              rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
              rollups: Seq[RollupInfo]): Secondary.Cookie =
  {
    val truthCopyInfo = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName) match {
      case None =>
        // The very top record - dataset_internal_name_map is missing.  Start everything new from new dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest SET broken_at = null, latest_secondary_data_version = 0, latest_secondary_lifecycle_stage = 'Unpublished' WHERE dataset_system_id = ? -- 20
        //   DELETE from falth.dataset_internal_name_map WHERE dataset_internal_name = ? -- 'alpha.20'
        logger.info("re-creating secondary dataset with new id")
        val newCopyInfo = pgu.datasetMapWriter.create(secondaryDatasetInfo.localeName)
        val newDatasetId = newCopyInfo.datasetInfo.systemId
        logger.info("new secondary dataset {} {}", secondaryDatasetInfo.internalName, newDatasetId.toString)
        pgu.secondaryDatasetMapWriter.createInternalNameMapping(secondaryDatasetInfo.internalName, newDatasetId)
        newCopyInfo
      case Some(dsId) =>
        // Find the very top record - dataset_internal_name_map.
        // Delete and recreate the copy with the same dataset id.
        // To use this resync, run this sql:
        //   UPDATE truth.secondary_manifest SET broken_at = null, latest_secondary_data_version = 0, latest_secondary_lifecycle_stage = 'Unpublished' WHERE dataset_system_id = ? -- 20
        pgu.datasetMapReader.datasetInfo(dsId).map { truthDatasetInfo =>
          pgu.datasetMapWriter.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber) match {
            case Some(copyInfo) =>
              logger.info("delete existing copy so that a new one can be created with the same ids {} {}", truthDatasetInfo.systemId.toString, secondaryCopyInfo.copyNumber.toString)
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
        }.getOrElse(throw new Exception(s"Cannot find existing dataset info.  You may manually delete dataset_internal_name_map record and start fresh ${secondaryDatasetInfo.internalName} ${dsId}"))
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

    val sidColumnInfo = newSchema.values.find(_.isSystemPrimaryKey == true).get

    for (iter <- rows) {
      // TODO: divide rows based on data size instead of number of rows.
      iter.grouped(resyncBatchSize).foreach { bi =>
        for (row: ColumnIdMap[SoQLValue] <- bi) {
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
    for (rollup <- rollups) {
      RollupCreatedOrUpdatedHandler(pgu, postUpdateTruthCopyInfo, rollup)
    }
    // re-create rollup tables
    updateRollups(pgu, postUpdateTruthCopyInfo, false)

    cookie
  }

  private def truthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String): Option[TruthCopyInfo] = {
    for {
      datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInternalName)
      truthDatasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      pgu.datasetMapReader.latest(truthDatasetInfo)
    }
  }

  private def updateRollups(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: TruthCopyInfo, dropFirst: Boolean) {
    val rm = new RollupManager(pgu, copyInfo)
    if (dropFirst) {
      rm.dropRollups(true)
    }
    pgu.datasetMapReader.rollups(copyInfo).foreach { ri =>
      rm.updateRollup(ri, copyInfo.dataVersion)
    }
  }

  private def prevettedLoader(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo) = {
      val truthSchema = pgu.datasetMapReader.schema(truthCopyInfo)
      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
      pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "user"))
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
        val pooled = DataSources.pooledDataSource(dataSource, null, overrideProps)
        new DSInfo(pooled, C3P0WrappedPostgresCopyIn) with Closeable {
          def close() {
            DataSources.destroy(pooled)
          }
        }
      case None =>
        new DSInfo(dataSource, PostgresCopyIn) with Closeable {
          def close() {}
        }
    }
  }
}
