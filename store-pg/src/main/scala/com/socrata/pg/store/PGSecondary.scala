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
  def shutdown() = ???

  def wantsWorkingCopies = ???

  def dropDataset(datasetInternalName: String, cookie: Secondary.Cookie) = ???

  def currentVersion(datasetInternalName: String, cookie: Secondary.Cookie) = ???

  def currentCopyNumber(datasetInternalName: String, cookie: Secondary.Cookie) = ???

  def snapshots(datasetInternalName: String, cookie: Secondary.Cookie) = ???

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Secondary.Cookie) = ???

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Secondary.Cookie, events: Iterator[Event[SoQLType, SoQLValue]]) = ???

  def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Secondary.Cookie, rows: _root_.com.rojoma.simplearm.Managed[Iterator[ColumnIdMap[SoQLValue]]]) = ???
}
