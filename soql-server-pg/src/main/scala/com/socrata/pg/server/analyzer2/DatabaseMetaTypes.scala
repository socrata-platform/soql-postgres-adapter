package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}

import com.socrata.prettyprint.prelude._

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.UserColumnId

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.SoQLTypeInfo

final class DatabaseMetaTypes extends MetaTypes {
  override type ResourceNameScope = InputMetaTypes#ResourceNameScope
  override type ColumnType = InputMetaTypes#ColumnType
  override type ColumnValue = InputMetaTypes#ColumnValue
  override type DatabaseTableNameImpl = CopyInfo
  override type DatabaseColumnNameImpl = ColumnInfo[ColumnType]

  val typeInfo = SoQLTypeInfo.metaProject[DatabaseMetaTypes]

  // ugh.. but making this stateful was the only way I could find to
  // do this.
  val provenanceMapper = new types.ProvenanceMapper[DatabaseMetaTypes] {
    private var counter = 0L;
    private val dtnMap = new scm.HashMap[Provenance, DatabaseTableName[DatabaseTableNameImpl]]

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseMetaTypes] = {
      dtnMap(prov)
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseMetaTypes]): Provenance = {
      val prov = Provenance(counter.toString)
      counter += 1
      dtnMap += prov -> dtn
      prov
    }
  }

  def rewriteFrom(analysis: SoQLAnalysis[InputMetaTypes], copyCache: CopyCache): SoQLAnalysis[DatabaseMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseMetaTypes](
      { dtn => DatabaseTableName(copyCache(dtn).get._1) }, // TODO proper error
      { case (dtn, DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(copyCache(dtn).get._2.get(userColumnId).get) // TODO proper errors
      },
      InputMetaTypes.provenanceMapper,
      provenanceMapper,
      typeInfo.updateProvenance
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
