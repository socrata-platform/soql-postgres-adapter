package com.socrata.pg.analyzer2.metatypes

import scala.collection.{mutable => scm}

import com.socrata.prettyprint.prelude._

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.{DatasetInternalName, DatasetId, CopyId, UserColumnId}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.SoQLTypeInfo2

import com.socrata.pg.analyzer2.SoQLValueDebugHelper

final class DatabaseMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = CopyInfo
  override type DatabaseColumnNameImpl = ColumnInfo[ColumnType]

  // don't care about literals here
  val updateProvenance = new SoQLTypeInfo2[DatabaseMetaTypes](false).updateProvenance _

  // ugh.. but making this stateful was the only way I could find to
  // do this.
  val provenanceMapper = new types.ProvenanceMapper[DatabaseMetaTypes] {
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]
    private val provMap = new scm.HashMap[(DatasetId, CopyId), Provenance]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseMetaTypes]): Provenance = {
      val provKey = (dtn.name.datasetInfo.systemId, dtn.name.systemId)
      provMap.get(provKey) match {
        case Some(existing) =>
          existing
        case None =>
          val prov = Provenance(dtnMap.size.toString)
          provMap += provKey -> prov
          dtnMap += prov -> dtn
          prov
      }
    }
  }

  def rewriteFrom[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = UserColumnId })](
    analysis: SoQLAnalysis[MT],
    copyCache: CopyCache[MT],
    fromProv: types.FromProvenance[MT]
  )(implicit changesOnlyLabels: MetaTypes.ChangesOnlyLabels[MT, DatabaseMetaTypes])
      : SoQLAnalysis[DatabaseMetaTypes] =
  {
    analysis.rewriteDatabaseNames[DatabaseMetaTypes](
      { dtn => DatabaseTableName(copyCache(dtn).get._1) }, // TODO proper error
      { case (dtn, DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(copyCache(dtn).get._2.get(userColumnId).get) // TODO proper errors
      },
      fromProv,
      provenanceMapper,
      updateProvenance
    )
  }
}

object DatabaseMetaTypes extends MetaTypeHelper[DatabaseMetaTypes] {
  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
    implicit object hasDocDBTN extends HasDoc[CopyInfo] {
      def docOf(cv: CopyInfo) = Doc(cv.datasetInfo.systemId.toString) ++ d"/" ++ Doc(cv.lifecycleStage.toString)

    }
    implicit object hasDocDBCN extends HasDoc[ColumnInfo[SoQLType]] {
      def docOf(cv: ColumnInfo[SoQLType]) = Doc(cv.userColumnId.toString)
    }
  }
}
