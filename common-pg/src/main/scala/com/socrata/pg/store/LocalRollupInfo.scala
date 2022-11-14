package com.socrata.pg.store

import com.socrata.Predicates
import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, RollupInfo}

import scala.util.Random

class LocalRollupInfo(copyInfo: CopyInfo, name: RollupName, soql: String, val tableName: String, val systemId: RollupId)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends RollupInfo(copyInfo, name, soql, None) {
  def updateName(newTableName: String) = new LocalRollupInfo(copyInfo, name, soql, newTableName, systemId)
}

object LocalRollupInfo {
  final val randomBits = 8


  // Generate the name for the actual rollup table
  // It is important that this is somewhat random so that we can drop/recreate a rollup table and not have names conflict
  def tableName(copyInfo: CopyInfo, name: RollupName): String = {
    val base = copyInfo.dataTableName + "_r_" + copyInfo.dataVersion + "_" + name.underlying.filter(Predicates.isAlphaNumericUnderscore) + "_"
    // clamp our base string to be max 63-randomBits since postgres table names are limited to 63 characters
    base.substring(0, Integer.min(base.length, 63 - randomBits)) + Random.alphanumeric.take(randomBits).mkString("")
  }
}
