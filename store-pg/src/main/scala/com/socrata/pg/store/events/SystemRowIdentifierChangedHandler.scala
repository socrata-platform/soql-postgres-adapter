package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{ColumnInfo, DatasetInfo}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import java.sql.Connection
import com.socrata.pg.store.{DatasetMeta, PGSecondaryLogger, PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.datacoordinator.id.DatasetId

/**
 * Handles SystemRowIdentifierChangedEvent
 */
case class SystemRowIdentifierChangedHandler(secDatasetInfo: DatasetInfo, dataVersion: Long, secColumnInfo: ColumnInfo[SoQLType], conn:Connection) {
  val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val datasetMeta = DatasetMeta.getMetadata(secDatasetInfo.internalName).get

  val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(new DatasetId(datasetMeta.datasetSystemId)).get
  val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

  val truthColumnInfo = pgu.datasetMapReader.schema(truthCopyInfo)(secColumnInfo.systemId)

  val newTruthColumnInfo = pgu.datasetMapWriter.setSystemPrimaryKey(truthColumnInfo)

  sLoader.makeSystemPrimaryKey(newTruthColumnInfo)

}
