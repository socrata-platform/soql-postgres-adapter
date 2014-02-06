package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}


// This only gets called once at dataset creation time.  We do not support it changing.
case class VersionColumnChangedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo, secColumnInfo: SecondaryColumnInfo[SoQLType]) {
  val truthColumnInfo = pgu.datasetMapReader.schema(truthCopyInfo)(secColumnInfo.systemId)

  val newTruthColumnInfo = pgu.datasetMapWriter.setVersion(truthColumnInfo)

  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  sLoader.makeVersion(newTruthColumnInfo)
}