package com.socrata.pg.store

import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.typesafe.config.Config
import com.socrata.pg.store.events._
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, _}
import com.socrata.datacoordinator.id.{RowId, UserColumnId, DatasetId}
import com.typesafe.scalalogging.slf4j.Logging
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage, ColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.secondary.VersionColumnChanged
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.pg.store.events.SystemRowIdentifierChangedHandler
import com.socrata.datacoordinator.secondary.RowIdentifierSet
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.RowIdentifierCleared
import com.socrata.pg.store.events.VersionColumnChangedHandler
import com.socrata.pg.store.events.WorkingCopyCreatedHandler
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.SnapshotDropped
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.pg.store.events.ColumnCreatedHandler
import com.socrata.datacoordinator.secondary.RowDataUpdated
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.socrata.datacoordinator.secondary.ColumnRemoved
import com.socrata.pg.store.events.WorkingCopyPublishedHandler
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.pg.SecondaryBase
import com.rojoma.simplearm.Managed
import java.util.concurrent.{CountDownLatch, TimeUnit}

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] with SecondaryBase with Logging {
  override val dsConfig = new DataSourceConfig(config, "database")

  private val finished = new CountDownLatch(1)

  private val tableDropper = startTableDropper()

  // Called when this process is shutting down (or being killed)
  def shutdown() {
    logger.debug("shutdown")
    finished.countDown()
    tableDropper.join()
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
    withPgu(None) { pgu =>
      val copyInfo = truthCopyInfo(pgu, datasetInternalName)
      val datasetId = copyInfo.datasetInfo.systemId
      pgu.datasetDropper.dropDataset(datasetId)
      pgu.commit()
    }
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(truthStoreDatasetInfo = None) { pgu =>
      _currentVersion(pgu, datasetInternalName, cookie)
    }
  }

  // Every set of changes increments the version number, so a given copy (number) may have
  // multiple versions over the course of it's life
  def _currentVersion(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    // every set of changes to a copy increments the version number
    // What happens when this is wrong? Almost certainly should turn into a resync
    logger.debug(s"currentVersion '${datasetInternalName}', (cookie: ${cookie})")

     truthCopyInfo(pgu, datasetInternalName).dataVersion
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withPgu(truthStoreDatasetInfo = None) { pgu =>
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

    truthCopyInfo(pgu, datasetInternalName).copyNumber
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
    logger.debug(s"dropCopy '${datasetInternalName}' (cookie: ${cookie}})")
    throw new UnsupportedOperationException("TODO later")
  }


  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    withPgu(Some(datasetInfo)) { pgu =>
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
      val e = wccEvents.next
      logger.debug("got working copy event: {}", e)
      e match {
        case WorkingCopyCreated(copyInfo) => WorkingCopyCreatedHandler(pgu, secondaryDatasetInfo, copyInfo)
        case otherOps => throw new UnsupportedOperationException("Unexpected operation")
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

    val rebuildIndex = remainingEvents.foldLeft(false) { (rebuildIndex, e) =>
      logger.debug("got event: {}", e)
      e match {
        case Truncated => // TODO: throw new UnsupportedOperationException("TODO later")
          rebuildIndex
        case ColumnCreated(secondaryColInfo) =>
          ColumnCreatedHandler(pgu, truthCopyInfo, secondaryColInfo)
          true
        case ColumnRemoved(secondaryColInfo) =>
          ColumnRemovedHandler(pgu, truthCopyInfo, secondaryColInfo)
          true
        case RowIdentifierSet(info) =>  // no-op
          rebuildIndex
        case RowIdentifierCleared(info) =>  // no-op
          rebuildIndex
        case SystemRowIdentifierChanged(secondaryColInfo) =>
          SystemRowIdentifierChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          rebuildIndex
        case VersionColumnChanged(secondaryColInfo) =>
          VersionColumnChangedHandler(pgu, truthCopyInfo, secondaryColInfo)
          rebuildIndex
        // TODO when dealing with dropped, figure out what version we have to updated afterwards and how that works...
        // maybe workingcopydropped is guaranteed to be the last event in a batch, and then we can skip updating the
        // version anywhere... ?
        case WorkingCopyDropped =>
          throw new UnsupportedOperationException("TODO later")
        case DataCopied =>
          throw new UnsupportedOperationException("TODO later")
        case SnapshotDropped(info) =>
          throw new UnsupportedOperationException("TODO later")
        case WorkingCopyPublished =>
          WorkingCopyPublishedHandler(pgu, truthCopyInfo)
          rebuildIndex
        case RowDataUpdated(ops) =>
          RowDataUpdatedHandler(pgu, truthCopyInfo, ops)
          rebuildIndex
        case LastModifiedChanged(lastModified) =>
          pgu.datasetMapWriter.updateLastModified(truthCopyInfo, lastModified)
          rebuildIndex
        case otherOps =>
          throw new UnsupportedOperationException("Unexpected operation")
      }
    }

    pgu.datasetMapWriter.updateDataVersion(truthCopyInfo, newDataVersion)

    if (rebuildIndex) {
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
      sLoader.createFullTextSearchIndex(schema.values)
    }

    cookie
  }


  // This is an expensive operation in that it is both time consuming as well as locking the data source for the duration
  // of the resync event. The resync event can come from either the DC, or originate from a ResyncSecondaryException being thrown
  // Incoming rows have their own ids already provided at this point
  // Need to record some state somewhere so that readers can know that a resync is underway
  // Backup code (Receiver class) touches this method as well via the receiveResync method
  // SoQL ID is only ever used for row ID
  def resync(datasetInfo: DatasetInfo, secondaryCopyInfo: SecondaryCopyInfo, schema: ColumnIdMap[SecondaryColumnInfo[SoQLType]], cookie: Secondary.Cookie, rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.info("resync (datasetInfo: {}, secondaryCopyInfo: {}, schema: {}, cookie: {})",
      datasetInfo, secondaryCopyInfo, schema, cookie)

    withPgu(Some(datasetInfo)) { pgu =>
      val cookieOut = _resync(pgu, datasetInfo, secondaryCopyInfo, schema, cookie, rows)
      pgu.commit()
      cookieOut
    }
  }

  def _resync(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
              secondaryDatasetInfo: DatasetInfo,
              secondaryCopyInfo: SecondaryCopyInfo,
              newSchema: ColumnIdMap[SecondaryColumnInfo[SoQLType]],
              cookie: Secondary.Cookie,
              rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Secondary.Cookie =
  {
    // create the dataset if it doesn't exist already
    val datasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(secondaryDatasetInfo.internalName).getOrElse {
      val newDatasetId = pgu.secondaryDatasetMapWriter.createDatasetOnly(secondaryDatasetInfo.localeName)
      pgu.secondaryDatasetMapWriter.createInternalNameMapping(secondaryDatasetInfo.internalName, newDatasetId)
      newDatasetId
    }

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get

    // delete the copy if it exists already
    pgu.datasetMapReader.copyNumber(truthDatasetInfo, secondaryCopyInfo.copyNumber).foreach(
      pgu.secondaryDatasetMapWriter.deleteCopy(_)
    )

    val truthCopyInfo =  pgu.datasetMapWriter.unsafeCreateCopy(truthDatasetInfo,
        secondaryCopyInfo.systemId,
        secondaryCopyInfo.copyNumber,
        TruthLifecycleStage.valueOf(secondaryCopyInfo.lifecycleStage.toString),
        secondaryCopyInfo.dataVersion)

    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(truthCopyInfo)

    newSchema.values.foreach { col =>
      ColumnCreatedHandler(pgu, truthCopyInfo, col)
    }

    val truthSchema: ColumnIdMap[ColumnInfo[SoQLType]] = pgu.datasetMapReader.schema(truthCopyInfo)
    val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
    val loader = pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "test-user"))

    val sidColumnInfo = newSchema.values.find(_.isSystemPrimaryKey == true).get

    for (iter <- rows; row: ColumnIdMap[SoQLValue] <- iter) {
      logger.trace("adding row: {}", row)
      val rowId = pgu.commonSupport.typeContext.makeSystemIdFromValue(row.get(sidColumnInfo.systemId).get)
      loader.insert(rowId, row)
    }
    loader.flush

    sLoader.createFullTextSearchIndex(truthSchema.values)

    cookie
  }

  private def truthCopyInfo(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInternalName: String): TruthCopyInfo = {
    val datasetId: DatasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetInternalName).get

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
    pgu.datasetMapReader.latest(truthDatasetInfo)
  }

  private def startTableDropper() = {
    val tableDropper = new Thread() {
      setName("pg-sec-table-dropper")
      override def run() {
        while(!finished.await(60, TimeUnit.SECONDS)) {
          try {
            withPgu(None) { pgu =>
              while(finished.getCount > 0 && pgu.tableCleanup.cleanupPendingDrops()) {
                pgu.commit()
              }
            }
          } catch {
            case e: Exception =>
              logger.error("Unexpected error while dropping secondary tables", e)
          }
        }
      }
    }
    tableDropper.start()
    tableDropper
  }
}