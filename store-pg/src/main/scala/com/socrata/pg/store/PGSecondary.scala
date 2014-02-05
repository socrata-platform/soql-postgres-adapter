package com.socrata.pg.store

import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.typesafe.config.Config
import com.socrata.pg.store.ddl.DatasetSchema
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo => TruthColumnInfo}
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
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import com.socrata.datacoordinator.id.DatasetId

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] {
  // Called when this process is shutting down (or being killed)
  def shutdown() {
    println("{}: shutdown (config: {})", this.getClass.toString, config)
    // noop
  }

  // Return true to get all the events from the stream of updates from the data-coordinator
  // Returning false here means that instead of a stream of updates from the DC, we will receive
  // the resync event instead.
  def wantsWorkingCopies: Boolean = {
    println("{}: wantsWorkingCopies", this.getClass.toString)
    true
  }

  // This is the last event we will every receive for a given dataset. Receiving this event means
  // that all data related to that dataset can/should be destroyed
  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) {
    // last thing you will get for a dataset.
    println("{}: dropDataset '{}' (cookie : {}) ", this.getClass.toString, datasetInternalName, cookie)
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
    println("{}: currentVersion '{}', (cookie: {})", datasetInternalName, cookie)
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
    println("{}: currentCopyNumber '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
    val datasetMeta = DatasetMeta.getMetadata(datasetInternalName).get

    val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
    val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)
    truthCopyInfo.systemId.underlying
  }

  // Currently there are zero-or-more snapshots, which are what you get when you publish a working copy when there is
  // an existing published working copy of that same dataset.
  // To NOOP this API, return an empty set.
  @deprecated("Not supporting snapshots beyond bare minimum required to function", since = "forever")
  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie): Set[Long] = {
    // if we a publish through version(); a snapshot "could" be created
    println("{}: snapshots '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    Set()
  }

  // Is only ever called as part of a resync.
  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie): Secondary.Cookie = {
    println("{}: dropCopy '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    throw new UnsupportedOperationException("TODO later")
  }


  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    withDb() { conn =>
       _version(datasetInfo, dataVersion, cookie, events, conn)
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
    // dataVersion is the version which cooresponds to the set of events which we are given; corresponds with the currentVersion
    //     - ignore this if the dataVersion <= currentVersion
    //     - stored in copy_map
    println("{}: version '{}' (datasetInfo: {}, dataVersion: {}, cookie: {}, events: {})",
      this.getClass.toString, datasetInfo, dataVersion, cookie, events)
    events.foreach { e =>
        println("got event: {}", e)
        e match {
          case Truncated => throw new UnsupportedOperationException("TODO later")
          case ColumnCreated(colInfo) => columnCreated(datasetInfo, dataVersion, colInfo, conn)
          case ColumnRemoved(info)  =>  columnRemoved(info, conn)
          case RowIdentifierSet(info) => Unit // no-op
          case RowIdentifierCleared(info) => Unit // no-op
          case SystemRowIdentifierChanged(info) => systemRowIdentifierChanged(info, conn)
          case VersionColumnChanged(info) => Unit // no-op
          case WorkingCopyCreated(copyInfo) => workingCopyCreated(datasetInfo, dataVersion, copyInfo, conn)
          case WorkingCopyDropped => throw new UnsupportedOperationException("TODO later")
          case DataCopied => throw new UnsupportedOperationException("TODO later")
          case SnapshotDropped(info) => throw new UnsupportedOperationException("TODO later")
          case WorkingCopyPublished => workingCopyPublished
          case RowDataUpdated(ops) => rowDataUpdated(ops, conn)
          case otherOps => throw new UnsupportedOperationException("Unexpected operation")
        }
      }
      cookie
    }


    // TODO2 we should be batching these
    def columnCreated(secDatasetInfo: DatasetInfo, secDatasetVersion: Long, secColInfo: SecondaryColumnInfo[SoQLType], conn:Connection) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

      val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

      val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
      val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

      val truthColInfo = pgu.datasetMapWriter.addColumn(
          truthCopyInfo,
          secColInfo.id, // user column id
          secColInfo.typ,
          secColInfo.id.underlying // + secColInfo.systemId.underlying // system column id
      )

      if (secColInfo.isSystemPrimaryKey) pgu.datasetMapWriter.setSystemPrimaryKey(truthColInfo)
      // noop these two, never set them?
//      if (secColInfo.isUserPrimaryKey) pgu.datasetMapWriter.setUserPrimaryKey(truthColInfo)
//      if (secColInfo.isVersion) pgu.datasetMapWriter.setVersion(truthColInfo)

      sLoader.addColumns(Seq(truthColInfo))
      if (truthColInfo.isSystemPrimaryKey) sLoader.makeSystemPrimaryKey(truthColInfo)
    }

    def columnRemoved(info: ColumnInfo[SoQLType], conn:Connection) = {
      throw new UnsupportedOperationException("TODO NOW optionally")
    }

    def systemRowIdentifierChanged(info: ColumnInfo[SoQLType], conn:Connection) = {
      throw new UnsupportedOperationException("TODO NOW")
    }

    def workingCopyCreated(datasetInfo: DatasetInfo, dataVersion: Long, copyInfo: CopyInfo, conn:Connection) = {
        if (copyInfo.copyNumber != 1)
            throw new UnsupportedOperationException("Cannot support making working copies beyond the first copy")
        val (pgu, copyInfoSecondary, sLoader) = DatasetSchema.createTable(conn, datasetInfo.localeName)
        if (copyInfoSecondary.copyNumber != 1)
          throw new UnsupportedOperationException("We only support one copy of a dataset!")
        DatasetMeta.setMetadata(DatasetMeta(datasetInfo.internalName, copyInfoSecondary.datasetInfo.systemId.underlying))
    }

    def workingCopyPublished = {
      throw new UnsupportedOperationException("TODO optional")
    }

    def rowDataUpdated(ops: Seq[Operation[SoQLValue]], conn:Connection) = {
      ops.foreach {
        case Insert(sid, row) =>  throw new UnsupportedOperationException("TODO NOW")
        case Update(sid, row) => throw new UnsupportedOperationException("TODO NOW")
        case Delete(sid) => throw new UnsupportedOperationException("TODO NOW")
      }
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
    println("{}: version '{}' (datasetInfo: {}, copyInfo: {}, schema: {}, cookie: {}, rows)",
      this.getClass.toString, datasetInfo, copyInfo, schema, cookie, rows)
    throw new UnsupportedOperationException("TODO later")
  }


  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) {
      conn =>
        conn.setAutoCommit(false)
        populateDatabase(conn)
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
