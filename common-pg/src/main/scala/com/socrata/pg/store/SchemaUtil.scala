package com.socrata.pg.store

import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLVersion, SoQLType}

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

  def addRequiredColumnsToSchema(querySchema: Map[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]],
                                 baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]]):
                                 Map[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = {

    val queryHasIdColumn = querySchema.values.exists(_.isSystemPrimaryKey)
    val queryHasVersionColumn = querySchema.values.exists(_.typ == SoQLVersion)
    val missingRequiredColumns = baseSchema.values.filter(cinfo =>
      (cinfo.isVersion && !queryHasVersionColumn) || (cinfo.isSystemPrimaryKey && !queryHasIdColumn))
    val querySchemaWithRequiredColumns = missingRequiredColumns.foldLeft(querySchema) { (map, cinfo) =>
    // Use negative column id to indicate columns not in soql select but are required by the system.
    // it will be ignored when we read the row from postgres ResultSet to memory.
      val cid = new com.socrata.datacoordinator.id.ColumnId(-(map.size + 1))
      map + (cid -> cinfo)
    }
    querySchemaWithRequiredColumns
  }
}
