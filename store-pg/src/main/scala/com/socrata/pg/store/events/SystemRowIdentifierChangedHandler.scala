package com.socrata.pg.store.events

import com.socrata.soql.types.{SoQLValue, SoQLType}
import java.sql.Connection
import com.socrata.pg.store._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.DatasetInfo

/**
 * Handles SystemRowIdentifierChangedEvent
 */
case class SystemRowIdentifierChangedHandler(secDatasetInfo: DatasetInfo, dataVersion: Long, secColumnInfo: ColumnInfo[SoQLType], conn:Connection) {
  val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val datasetId: DatasetId = new PostgresDatasetInternalNameMapReader(conn).datasetIdForInternalName(secDatasetInfo.internalName).get

  val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
  val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

  val truthColumnInfo = pgu.datasetMapReader.schema(truthCopyInfo)(secColumnInfo.systemId)

  val newTruthColumnInfo = pgu.datasetMapWriter.setSystemPrimaryKey(truthColumnInfo)

  sLoader.makeSystemPrimaryKey(newTruthColumnInfo)

}
