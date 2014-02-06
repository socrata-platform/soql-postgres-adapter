package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo }

/**
 * Handles WorkingCopyCreated Event
 */
case class WorkingCopyCreatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo:SecondaryDatasetInfo, copyInfo:SecondaryCopyInfo) {

  if (copyInfo.copyNumber != 1) {
    throw new UnsupportedOperationException("Cannot support making working copies beyond the first copy")
  }

  val (copyInfoSecondary:TruthCopyInfo, sLoader) = createDataset(pgu, datasetInfo.localeName)

  if (copyInfoSecondary.copyNumber != 1) {
    throw new UnsupportedOperationException("We only support one copy of a dataset!")
  }

  pgu.datasetInternalNameMapWriter.create(datasetInfo.internalName, copyInfoSecondary.datasetInfo.systemId)

  private def createDataset(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], locale:String):(TruthCopyInfo, SchemaLoader[SoQLType]) = {
    val copyInfo = pgu.datasetMapWriter.create(locale)
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    // TODO figure out best way to avoid creating unused log and audit tables.
    sLoader.create(copyInfo)
    (copyInfo, sLoader)
  }

}
