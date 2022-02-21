package com.socrata.pg.store

import java.security.MessageDigest

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, RollupInfo}

class LocalRollupInfo(copyInfo: CopyInfo, name: RollupName, soql: String, val tableName: String)(implicit tag: com.socrata.datacoordinator.truth.metadata.`-impl`.Tag) extends RollupInfo(copyInfo, name, soql) {
  def updateName(newTableName: String) = new LocalRollupInfo(copyInfo, name, soql, newTableName)
}

object LocalRollupInfo {
  def tableName(copyInfo: CopyInfo, name: RollupName): String = {
    val sha1 = MessageDigest.getInstance("SHA-1")
    // we have a 63 char limit on table names, so just taking a prefix.  It only has to be
    // unique within a single dataset copy.
    val bytes = 8
    val nameHash = sha1.digest(name.underlying.getBytes("UTF-8")).take(bytes).map("%02x" format _).mkString

    copyInfo.dataTableName + "_r_" + copyInfo.dataVersion + "_" + nameHash
  }
}
