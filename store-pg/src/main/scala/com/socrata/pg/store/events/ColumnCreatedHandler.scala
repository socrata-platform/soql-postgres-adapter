package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryLogger, PGSecondaryUniverse, SoQLSystemColumns, StandardDatasetMapLimits}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.secondary.{ColumnInfo => SecondaryColumnInfo}
import com.socrata.datacoordinator.truth.metadata.{LifecycleStage, CopyInfo => TruthCopyInfo}
import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.id.UserColumnId
import com.rojoma.simplearm.v2._


// TODO2 we should be batching these
case class ColumnCreatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                truthCopyInfo: TruthCopyInfo,
                                secColInfo: SecondaryColumnInfo[SoQLType]) {
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val truthColInfo = pgu.datasetMapWriter.addColumnWithId(
    secColInfo.systemId,
    truthCopyInfo,
    secColInfo.id, // user column id
    secColInfo.fieldName,
    secColInfo.typ,
    physicalColumnBaseBase(secColInfo.id.underlying, isSystemColumnId(secColInfo.id)), // system column id
    None // not going to store computation strategies
  )

  if (secColInfo.isSystemPrimaryKey) pgu.datasetMapWriter.setSystemPrimaryKey(truthColInfo)
  if (secColInfo.isUserPrimaryKey) pgu.datasetMapWriter.setUserPrimaryKey(truthColInfo)
  if (secColInfo.isVersion) pgu.datasetMapWriter.setVersion(truthColInfo)

  sLoader.addColumns(Seq(truthColInfo))
  if (truthColInfo.isSystemPrimaryKey) sLoader.makeSystemPrimaryKey(truthColInfo)

  for { published <- pgu.datasetMapReader.lookup(truthCopyInfo.datasetInfo, LifecycleStage.Published)
        fieldName <- secColInfo.fieldName } {
    val idxdirectives = pgu.datasetMapWriter.indexDirectives(published, Some(fieldName))
    for (idxdt <- idxdirectives) {
      pgu.datasetMapWriter.createOrUpdateIndexDirective(truthColInfo, idxdt.directive)
    }
  }

  // TODO this is copy and paste from SoQLCommon ...
  private def physicalColumnBaseBase(nameHint: String, systemColumn: Boolean): String =
    AsciiIdentifierFilter(List(if(systemColumn) "s" else "u", nameHint)).
      take(StandardDatasetMapLimits.maximumPhysicalColumnBaseLength).
      replaceAll("_+$", "").
      toLowerCase

  private def isSystemColumnId(name: UserColumnId) =
    SoQLSystemColumns.isSystemColumnId(name)
}
