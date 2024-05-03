package com.socrata.pg.store

import com.socrata.datacoordinator.id.{UserColumnId, DatasetResourceName}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.analyzer.QualifiedColumnName
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, ResourceName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

abstract class SecondaryManagerBase(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {

  def toDatasetContext(columnInfos: Iterable[ColumnInfo[SoQLType]]): DatasetContext[SoQLType] = {
    new DatasetContext[SoQLType] {
      val schema: OrderedMap[ColumnName, SoQLType] =
        OrderedMap(columnInfos.foldLeft(Seq.empty[(ColumnName, SoQLType)]) { (acc, ci) =>
          ci.fieldName match {
            case Some(name) =>
              acc :+ (name -> ci.typ)
            case None =>
              acc
          }
        }: _*)
    }
  }

  def columnNameToColumnIdMap(columnInfos: Iterable[ColumnInfo[SoQLType]]): Map[QualifiedColumnName, UserColumnId] = {
    columnInfos.foldLeft(Map.empty[QualifiedColumnName, UserColumnId]) { (acc, ci) =>
      ci.fieldName match {
        case Some(name) =>
          acc + (QualifiedColumnName(None, name) -> ci.userColumnId)
        case None =>
          acc
      }
    }
  }

  def toTypeRepMap(columnInfos: Iterable[ColumnInfo[SoQLType]]): Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]] = {
    columnInfos.map { ci =>
      ci.typ -> pgu.commonSupport.repFor(ci)
    }.toMap
  }

  protected def getDsSchema(resourceName: DatasetResourceName): ColumnIdMap[ColumnInfo[SoQLType]] = {
    val dsInfo = pgu.datasetMapReader.datasetInfoByResourceName(resourceName).get
    val copyInfo = pgu.datasetMapReader.latest(dsInfo)
    getDsSchema(copyInfo)
  }

  protected def getDsSchema(copyInfo: CopyInfo): ColumnIdMap[ColumnInfo[SoQLType]] = {
    for (readCtx <- pgu.datasetReader.openDataset(copyInfo)) {
      readCtx.schema
    }
  }

  /**
   * For analyzing the rollup query, we need to map the dataset schema column ids to the "_" prefixed
   * version of the name that we get, designed to ensure the column name is valid soql
   * and doesn't start with a number.
   */
  protected def columnIdToPrefixNameMap(cid: UserColumnId): ColumnName = {
    val name = cid.underlying
    new ColumnName(if (name(0) == ':') name else "_" + name)
  }
}
