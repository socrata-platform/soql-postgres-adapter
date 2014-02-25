package com.socrata.pg.store

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLVersion, SoQLType}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}

object SchemaUtil {

  def systemToUserColumnMap(columnIdMap: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]]):
      Map[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.id.UserColumnId] = {

    columnIdMap.values.map { (cinfo: com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]) =>
      (cinfo.systemId, cinfo.userColumnId)
    }.toMap
  }

  def userToSystemColumnMap(columnIdMap: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]]):
    Map[com.socrata.datacoordinator.id.UserColumnId, com.socrata.datacoordinator.id.ColumnId] = {

    columnIdMap.values.map { (cinfo: com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]) =>
      (cinfo.userColumnId, cinfo.systemId)
    }.toMap
  }
}
