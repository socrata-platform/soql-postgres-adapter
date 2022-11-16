package com.socrata.pg.store

import com.socrata.Predicates
import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, RollupInfo}

class LocalRollupInfo(copyInfo: CopyInfo, name: RollupName, soql: String, val tableName: String, val systemId: RollupId)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends RollupInfo(copyInfo, name, soql, None) {
  def updateName(newTableName: String) = new LocalRollupInfo(copyInfo, name, soql, newTableName, systemId)
}

object LocalRollupInfo {
  // Generate the name for the actual rollup table
  // It is important that this is somewhat random so that we can drop/recreate a rollup table and not have names conflict
  // Note that table name should be lowercase, as this is an assumption throughout the stack
  def tableName(copyInfo: CopyInfo, name: RollupName, sequenceToAppend: String): String = {
    val base = copyInfo.dataTableName + "_r_" + copyInfo.dataVersion + "_" + name.underlying.filter(Predicates.isAlphaNumericUnderscore).toLowerCase + "_"
    // clamp our base string to be max 63-randomBits since postgres table names are limited to 63 characters
    (base.take(63 - sequenceToAppend.length) + sequenceToAppend)
  }
}
