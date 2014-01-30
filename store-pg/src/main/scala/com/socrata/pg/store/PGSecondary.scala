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
    false
  }

  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) {
    println("{}: dropDataset '{}' (cookie : {}) ", this.getClass.toString, datasetInternalName, cookie)
  }

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    println("{}: currentVersion '{}', (cookie: {})", datasetInternalName, cookie)
    0
  }

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie): Long = {
    println("{}: currentCopyNumber '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    0
  }

  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie): Set[Long] = {
    println("{}: snapshots '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    null
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie): Secondary.Cookie = {
    println("{}: dropCopy (aka 'snapshop') '{}' (cookie: {})", this.getClass.toString, datasetInternalName, cookie)
    cookie
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Secondary.Cookie = {
    println("{}: version '{}' (datasetInfo: {}, dataVersion: {}, cookie: {}, events: {})",
      this.getClass.toString, datasetInfo, dataVersion, cookie, events)
    cookie
  }

  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Secondary.Cookie, rows: _root_.com.rojoma.simplearm.Managed[Iterator[ColumnIdMap[SoQLValue]]]): Secondary.Cookie = {
    println("{}: version '{}' (datasetInfo: {}, copyInfo: {}, schema: {}, cookie: {}, rows)",
      this.getClass.toString, datasetInfo, copyInfo, schema, cookie, rows)
    cookie
  }
}
