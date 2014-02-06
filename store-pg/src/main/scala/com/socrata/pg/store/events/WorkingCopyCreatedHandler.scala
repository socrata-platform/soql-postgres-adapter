package com.socrata.pg.store.events

import java.sql.Connection
import com.socrata.pg.store.{PostgresDatasetInternalNameMapWriter, PGSecondaryLogger, PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo }

/**
 * Handles WorkingCopyCreated Event
 */
case class WorkingCopyCreatedHandler(datasetInfo:SecondaryDatasetInfo, dataVersion: Long, copyInfo:SecondaryCopyInfo, conn:Connection) {

  if (copyInfo.copyNumber != 1) {
    throw new UnsupportedOperationException("Cannot support making working copies beyond the first copy")
  }

  val (pgu, copyInfoSecondary:TruthCopyInfo, sLoader) = createDataset(conn, datasetInfo.localeName)

  if (copyInfoSecondary.copyNumber != 1) {
    throw new UnsupportedOperationException("We only support one copy of a dataset!")
  }

  new PostgresDatasetInternalNameMapWriter(conn).create(datasetInfo.internalName, copyInfoSecondary.datasetInfo.systemId)

  private def createDataset(conn:Connection, locale:String):(PGSecondaryUniverse[SoQLType, SoQLValue], TruthCopyInfo, SchemaLoader[SoQLType]) = {
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
    val copyInfo = pgu.datasetMapWriter.create(locale)
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(copyInfo)
    (pgu, copyInfo, sLoader)
  }

}
