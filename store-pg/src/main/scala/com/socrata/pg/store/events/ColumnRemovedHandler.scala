package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, DatasetCopyContext}

case class ColumnRemovedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo, secColInfo: SecondaryColumnInfo[SoQLType]) {
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val truthCopyContext = new DatasetCopyContext[SoQLType](truthCopyInfo, pgu.datasetMapReader.schema(truthCopyInfo))
  val truthColInfo = truthCopyContext.thaw().columnInfo(secColInfo.systemId)

  pgu.datasetMapWriter.dropColumn(truthColInfo)
  sLoader.dropColumns(Iterable(truthColInfo))
}