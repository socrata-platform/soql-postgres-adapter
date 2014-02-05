package com.socrata.pg.store

import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.typesafe.config.Config
import com.socrata.pg.store.events.WorkingCopyCreatedHandler
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo => TruthColumnInfo, DatasetCopyContext}
import com.socrata.datacoordinator.secondary.VersionColumnChanged
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.RowIdentifierSet
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.RowIdentifierCleared
import com.socrata.datacoordinator.secondary.Update
import com.socrata.datacoordinator.secondary.SystemRowIdentifierChanged
import com.socrata.datacoordinator.secondary.SnapshotDropped
import com.socrata.datacoordinator.secondary.Delete
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.secondary.RowDataUpdated
import com.socrata.datacoordinator.secondary.ColumnRemoved
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.Insert
import com.socrata.datacoordinator.id.{RowId, UserColumnId, DatasetId}
import com.socrata.soql.brita.AsciiIdentifierFilter
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] with Logging {

  // Called when this process is shutting down (or being killed)
  def shutdown() {
    logger.debug("shutdown (config: {})", config)
    // no-op
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
    throw new UnsupportedOperationException("TODO later")
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withDb() { conn =>
      _currentVersion(datasetInternalName, cookie, conn)
    }
  }

  // Every set of changes increments the version number, so a given copy (number) may have
  // multiple versions over the course of it's life
  protected[store] def _currentVersion(datasetInternalName: String, cookie: Secondary.Cookie, conn: Connection): Long = {
    // every set of changes to a copy increments the version number
    // What happens when this is wrong? Almost certainly should turn into a resync
    logger.debug(s"currentVersion '${datasetInternalName}', (cookie: ${cookie})")
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )

    val datasetMeta = DatasetMeta.getMetadata(datasetInternalName).get

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
    val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
    truthCopyInfo.dataVersion
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    withDb() { conn =>
      _currentCopyNumber(datasetInternalName, cookie, conn)
    }
  }

  // Current copy number is incremented every time a copy is made within the data coordinator
  // Publishing or snapshotting does not increment the copy number
  // The datasetmap contains both the current version number and current copy number and should be consulted to determine
  // the copy number value to return in this method. The datasetmap resides in the metadata db and can be looked up by datasetInternalName
  protected[store] def _currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie, conn: Connection): Long = {
    // Always incremented a working copy is made in the datacoordinator
    // the current copy number should always come out of the resync call or WorkingCopyCreatedEvent
    // still need the mapping from ds internal name to the copy number
    //
    // if we do not do working copies; we *should* receive a resync event instead of a publish event
    //
    // What happens if this is wrong? almost certainly it would turn into a resync
    logger.debug(s"currentCopyNumber '${datasetInternalName}' (cookie: ${cookie})")
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
    val datasetMeta = DatasetMeta.getMetadata(datasetInternalName).get

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
    val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
    truthCopyInfo.systemId.underlying
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
    withDb() { conn =>
       val cookieOut = _version(datasetInfo, dataVersion, cookie, events, conn)
       conn.commit()
       cookieOut
    }
  }
    /// NEED datasetName -> currentCopyNum
  /// datasetName -> in_async

  // The main method by which data will be sent to this API.
  // workingCopyCreated event is the (first) event by which this method will be called
  // "You always update the latest copy through the version method"
  // A separate event will be passed to this method for actually copying the data
  // If working copy already exists and we receive a workingCopyCreated event is received, then a resync event/exception should fire
  // Publishing a working copy promotes that working copy to a published copy. There should no longer be a working copy after publishing
  protected[store] def _version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]], conn:Connection): Secondary.Cookie = {
    // How do we get the copyInfo? dataset_map
    //  - One of the events that comes through here will be working copy created; it must be the first if it does; separate event for actually copying
    //    the data
    //  - Always update the latest copy through version if no working copy created event is passed in
    //  - If you have a working copy and you get a working copy created event; resync
    //  - If you don't have a working copy and you get a publish event; resync
    //  - Unpublished => Working Copy

    // rowVersion is given through the event
    // dataVersion is the version which corresponds to the set of events which we are given; corresponds with the currentVersion
    //     - ignore this if the dataVersion <= currentVersion
    //     - stored in copy_map
    logger.debug("version (datasetInfo: {}, dataVersion: {}, cookie: {}, events: {})",
      datasetInfo, dataVersion.asInstanceOf[AnyRef], cookie, events)

    // TODO check version beforehand

    events.foreach { e =>
        logger.debug("got event: {}", e)
        e match {
          case Truncated => throw new UnsupportedOperationException("TODO later")
          case ColumnCreated(colInfo) => columnCreated(datasetInfo, dataVersion, colInfo, conn)
          case ColumnRemoved(info)  =>  columnRemoved(info, conn)
          case RowIdentifierSet(info) => Unit // no-op
          case RowIdentifierCleared(info) => Unit // no-op
          case SystemRowIdentifierChanged(colInfo) => systemRowIdentifierChanged(datasetInfo, dataVersion, colInfo, conn)
          case VersionColumnChanged(colInfo) => versionColumnChanged(datasetInfo, dataVersion, colInfo, conn)
          case WorkingCopyCreated(copyInfo) => WorkingCopyCreatedHandler(datasetInfo, dataVersion, copyInfo, conn)
          case WorkingCopyDropped => throw new UnsupportedOperationException("TODO later")
          case DataCopied => throw new UnsupportedOperationException("TODO later")
          case SnapshotDropped(info) => throw new UnsupportedOperationException("TODO later")
          case WorkingCopyPublished => workingCopyPublished(datasetInfo, dataVersion, conn)
          case RowDataUpdated(ops) => rowDataUpdated(datasetInfo, dataVersion, ops, conn)
          case otherOps => throw new UnsupportedOperationException("Unexpected operation")
        }
      }

      // TODO update version afterwards
      cookie
    }


  // TODO this is copy and paste from SoQLCommon ...
  private def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean): String =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", nameHint)).
      take(StandardDatasetMapLimits.maximumPhysicalColumnBaseLength).
      replaceAll("_+$", "").
      toLowerCase

  private def isSystemColumnId(name: UserColumnId) =
    SoQLSystemColumns.isSystemColumnId(name)


  // TODO2 we should be batching these
    def columnCreated(secDatasetInfo: DatasetInfo, secDatasetVersion: Long, secColInfo: SecondaryColumnInfo[SoQLType], conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

      val truthColInfo = pgu.datasetMapWriter.addColumnWithId(
          secColInfo.systemId,
          truthCopyInfo,
          secColInfo.id, // user column id
          secColInfo.typ,
          physicalColumnBaseBase(secColInfo.id.underlying, isSystemColumnId(secColInfo.id)) // system column id
      )

      if (secColInfo.isSystemPrimaryKey) pgu.datasetMapWriter.setSystemPrimaryKey(truthColInfo)
      // no-op these two, never set them?
      if (secColInfo.isUserPrimaryKey) pgu.datasetMapWriter.setUserPrimaryKey(truthColInfo)
      if (secColInfo.isVersion) pgu.datasetMapWriter.setVersion(truthColInfo)

      sLoader.addColumns(Seq(truthColInfo))
      if (truthColInfo.isSystemPrimaryKey) sLoader.makeSystemPrimaryKey(truthColInfo)
    }

    def columnRemoved(info: ColumnInfo[SoQLType], conn:Connection) = {
      throw new UnsupportedOperationException("TODO NOW optionally")
    }

    // This only gets called once at dataset creation time.  We do not support it changing.
    def systemRowIdentifierChanged(secDatasetInfo: DatasetInfo, dataVersion: Long, secColumnInfo: ColumnInfo[SoQLType], conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

      val truthColumnInfo = pgu.datasetMapReader.schema(truthCopyInfo)(secColumnInfo.systemId)

      val newTruthColumnInfo = pgu.datasetMapWriter.setSystemPrimaryKey(truthColumnInfo)

      sLoader.makeSystemPrimaryKey(newTruthColumnInfo)
    }

    // This only gets called once at dataset creation time.  We do not support it changing.
    def versionColumnChanged(secDatasetInfo: DatasetInfo, dataVersion: Long, secColumnInfo: ColumnInfo[SoQLType], conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

      val truthColumnInfo = pgu.datasetMapReader.schema(truthCopyInfo)(secColumnInfo.systemId)

      val newTruthColumnInfo = pgu.datasetMapWriter.setVersion(truthColumnInfo)

      sLoader.makeVersion(newTruthColumnInfo)
    }

    def workingCopyPublished(secDatasetInfo: DatasetInfo, dataVersion: Long, conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

      pgu.datasetMapWriter.publish(truthCopyInfo)
    }

    def rowDataUpdated(datasetInfo: DatasetInfo, dataVersion: Long, ops: Seq[Operation[SoQLValue]], conn:Connection) = {
      ops.foreach { o =>
        logger.debug("Got row operation: {}", o)
        o match {
          case Insert(sid, row) => rowInsert(datasetInfo, sid, row, conn)
          case Update(sid, row) => throw new UnsupportedOperationException("TODO NOW")
          case Delete(sid) => throw new UnsupportedOperationException("TODO NOW")
        }
      }
    }


    def rowInsert(secDatasetInfo: DatasetInfo, systemId: RowId, row: Row[SoQLValue], conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
      val truthSchema = pgu.datasetMapReader.schema(truthCopyInfo)

      val copyCtx = new DatasetCopyContext[SoQLType](truthCopyInfo, truthSchema)
      val loader = pgu.prevettedLoader(copyCtx, pgu.logger(truthCopyInfo.datasetInfo, "test-user"))

      loader.insert(systemId, row)
      loader.flushInserts()
    }



  // This is an expensive operation in that it is both time consuming as well as locking the data source for the duration
  // of the resync event. The resync event can come from either the DC, or originate from a ResyncSecondaryException being thrown
  // Incoming rows have their own ids already provided at this point
  // Need to record some state somewhere so that readers can know that a resync is underway
  // Backup code (Receiver class) touches this method as well via the receiveResync method
  // SoQL ID is only ever used for row ID
  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Secondary.Cookie, rows: _root_.com.rojoma.simplearm.Managed[Iterator[ColumnIdMap[SoQLValue]]]): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    logger.debug("resync (datasetInfo: {}, copyInfo: {}, schema: {}, cookie: {}, rows)",
      datasetInfo, copyInfo, schema, cookie, rows)
    throw new UnsupportedOperationException("TODO later")
  }


  @volatile var populatedDb = false
  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) {
      conn =>
        conn.setAutoCommit(false)
        if (!populatedDb) {
          populateDatabase(conn)
          populatedDb = true
        }
        f(conn)
    }
  }

  def populateDatabase(conn: Connection) {
    val sql = DatabasePopulator.createSchema()
    using(conn.createStatement()) {
      stmt =>
        stmt.execute(sql)
    }
  }
}
