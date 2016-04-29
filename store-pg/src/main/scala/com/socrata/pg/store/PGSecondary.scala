package com.socrata.pg.store

import java.io.Closeable
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.socrata.pg.BuildInfo
import com.mchange.v2.c3p0.DataSources
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo, CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage}
import com.socrata.datacoordinator.truth.universe.sql.{C3P0WrappedPostgresCopyIn, PostgresCopyIn}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.SecondaryBase
import com.socrata.pg.config.StoreConfig
import com.socrata.pg.store.events.{ColumnCreatedHandler, WorkingCopyPublishedHandler, _}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.thirdparty.typesafeconfig.C3P0Propertizer
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.postgresql.ds.PGSimpleDataSource

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] with SecondaryBase with Logging {
  logger.info(BuildInfo.toJson)

  val storeConfig = new StoreConfig(config, "")
  override val dsConfig =  storeConfig.database

  private val dsInfo = dataSourceFromConfig(dsConfig)
  private val finished = new CountDownLatch(1)
  private val tableDropper = startTableDropper()
  private val resyncBatchSize = storeConfig.resyncBatchSize
  private val tableDropTimeoutSeconds: Long = 60

  val postgresUniverseCommon = new PostgresUniverseCommon(TablespaceFunction(storeConfig.tablespace), dsInfo.copyIn)

  // Called when this process is shutting down (or being killed)
  def shutdown(): Unit = {
    logger.info("shutting down {} {} ...", dsConfig.host, dsConfig.database)
    finished.countDown()
    tableDropper.join()
    dsInfo.close()
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

  def version(datasetInfo: DatasetInfo,
              dataVersion: Long,
              cookie: Secondary.Cookie,
              events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
     val cookieOut = doVersion(pgu, datasetInfo, dataVersion, cookie, events)
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
    val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

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
        s"Current version ${truthCopyInfo.dataVersion}, next version ${newDataVersion} but should be ${dvExpect}")
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
        case FieldNameUpdated(secondaryColInfo) =>
          FieldNameUpdatedHandler(pgu, truthCopyInfo, secondaryColInfo)
          (rebuildIndex, false,  None)
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
          logger.info("drop snapshot system id - {}, copy number - {}",
            info.systemId.toString(), info.copyNumber.toString)
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
        case otherOps: Event[SoQLType,SoQLValue] =>
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
      updateRollups(pgu)(postUpdateTruthCopyInfo)
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
             isLatestLivingCopy: Boolean): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.info("resync (datasetInfo: {}, secondaryCopyInfo: {}, schema: {}, cookie: {})",
      datasetInfo, secondaryCopyInfo, schema, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      ResyncHandler(pgu, datasetInfo, secondaryCopyInfo).doResync(schema, rows, rollups,
                                                                  resyncBatchSize, updateRollups(pgu))
      pgu.commit()
    }
    cookie
  }

  // Is only ever called as part of a resync.
  def dropCopy(datasetInfo: DatasetInfo,
               secondaryCopyInfo: SecondaryCopyInfo,
               cookie: Secondary.Cookie, isLatestCopy: Boolean): Secondary.Cookie = {
    logger.info("droping copy (datasetInfo: {}, secondaryCopyInfo: {}, cookie: {})",
      datasetInfo, secondaryCopyInfo, cookie)
    withPgu(dsInfo, Some(datasetInfo)) { pgu =>
      ResyncHandler(pgu, datasetInfo, secondaryCopyInfo).doDropCopy(isLatestCopy)
      pgu.commit()
    }
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

  private def updateRollups(pgu: PGSecondaryUniverse[SoQLType, SoQLValue])(copyInfo: TruthCopyInfo): Unit = {
    val rm = new RollupManager(pgu, copyInfo)
    rm.dropRollups(immediate = true)
    pgu.datasetMapReader.rollups(copyInfo).foreach { ri =>
      rm.updateRollup(ri, copyInfo.dataVersion)
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
}
