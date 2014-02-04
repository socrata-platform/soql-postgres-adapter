package com.socrata.pg.store

import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.typesafe.config.Config

/**
 * Postgres Secondary Store Implementation
 */
class PGSecondary(val config: Config) extends Secondary[SoQLType, SoQLValue] {
  def shutdown() {
    println("{}: shutdown (config: {})", this.getClass.toString, config)
  }

  def wantsWorkingCopies: Boolean = {
    println("{}: wantsWorkingCopies", this.getClass.toString)
    true
  }

  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) {
    // last thing you will get for a dataset.
    println("{}: dropDataset '{}' (cookie : {}) ", this.getClass.toString, datasetInternalName, cookie)
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    // every set of changes to a copy increments the version number
    // What happens when this is wrong? Almost certaintly should turn into a resync
    println("{}: currentVersion '{}', (cookie: {})", datasetInternalName, cookie)
    0
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    // Always incremented a working copy is made in the datacoordinator
    // the current copy number should always come out of the resync call or WorkingCopyCreatedEvent
    // still need the mapping from ds internal name to the copy number
    //
    // if we do not do working copies; we *should* receive a resync event instead of a publish event
    //
    // What happens if this is wrong? almost certainly it would turn into a resync
    println("{}: currentCopyNumber '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    0
  }

  @deprecated
  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie): Set[Long] = {
    // if we a publish through version(); a snapshot "could" be created
    println("{}: snapshots '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    Set()
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie): Secondary.Cookie = {
    println("{}: dropCopy '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    cookie
  }

  /// NEED datasetName -> currentCopyNum
  /// datasetName -> in_async

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
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

    events.foreach {
      case RowDataUpdated(ops) =>
        ops.foreach {
          case Insert(sid, row) =>  throw new UnsupportedOperationException
          case Update(sid, row) => throw new UnsupportedOperationException
          case Delete(sid) => throw new UnsupportedOperationException
        }
      case WorkingCopyCreated(copyInfo) => throw new UnsupportedOperationException
      case ColumnCreated(info) => throw new UnsupportedOperationException
      case SnapshotDropped(info) => throw new UnsupportedOperationException
      case Truncated => throw new UnsupportedOperationException
      case ColumnRemoved(_)  =>  throw new UnsupportedOperationException
      case WorkingCopyCreated(_) => throw new UnsupportedOperationException
      case RowIdentifierSet(_) => throw new UnsupportedOperationException
      case RowIdentifierCleared(_) => throw new UnsupportedOperationException
      case otherOps => throw new UnsupportedOperationException
    }

    cookie
  }

  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Secondary.Cookie, rows: _root_.com.rojoma.simplearm.Managed[Iterator[ColumnIdMap[SoQLValue]]]): Secondary.Cookie = {
    // should tell us the new copy number
    // We need to perform some accounting here to make sure readers know a resync is in process
    println("{}: version '{}' (datasetInfo: {}, copyInfo: {}, schema: {}, cookie: {}, rows)",
      this.getClass.toString, datasetInfo, copyInfo, schema, cookie, rows)
    cookie
  }
}
