package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.DatasetInfo
import java.sql.Connection
import com.socrata.pg.store.{PostgresDatasetInternalNameMapReader, PGSecondaryLogger, PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.id.DatasetId

/**
 * Handles the WorkingCopyPublishedEvent
 */
case class WorkingCopyPublishedHandler(secDatasetInfo: DatasetInfo, dataVersion: Long, conn:Connection) {
  val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val datasetId: DatasetId = new PostgresDatasetInternalNameMapReader(conn).datasetIdForInternalName(secDatasetInfo.internalName).get

  val truthDatasetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
  val truthCopyInfo = pgu.datasetMapReader.latest(truthDatasetInfo)

  pgu.datasetMapWriter.publish(truthCopyInfo)
}
