package com.socrata.pg.server.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.UserColumnId

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.types.obfuscation.CryptProvider

final abstract class DatabaseMetaTypes extends MetaTypes {
  override type ResourceNameScope = InputMetaTypes#ResourceNameScope
  override type ColumnType = InputMetaTypes#ColumnType
  override type ColumnValue = InputMetaTypes#ColumnValue
  override type DatabaseTableNameImpl = CopyInfo
  override type DatabaseColumnNameImpl = ColumnInfo[ColumnType]
}

object DatabaseMetaTypes extends MetaTypeHelper[DatabaseMetaTypes] {
  def rewriteFrom(analysis: SoQLAnalysis[InputMetaTypes], copyCache: CopyCache): SoQLAnalysis[DatabaseMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseMetaTypes](
      { dtn => DatabaseTableName(copyCache(dtn).get._1) }, // TODO proper error
      { case (dtn, DatabaseColumnName(userColumnId)) =>
        DatabaseColumnName(copyCache(dtn).get._2.get(userColumnId).get) // TODO proper errors
      }
    )
  }

  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
    implicit object hasDocDBTN extends HasDoc[CopyInfo] {
      def docOf(cv: CopyInfo) = Doc(cv.datasetInfo.systemId.toString) ++ d"/" ++ Doc(cv.lifecycleStage.toString)

    }
    implicit object hasDocDBCN extends HasDoc[ColumnInfo[SoQLType]] {
      def docOf(cv: ColumnInfo[SoQLType]) = Doc(cv.userColumnId.toString)
    }
  }
}
