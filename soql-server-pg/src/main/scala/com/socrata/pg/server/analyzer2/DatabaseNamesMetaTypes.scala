package com.socrata.pg.server.analyzer2

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.UserColumnId

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.SoQLValue
import com.socrata.soql.types.obfuscation.CryptProvider

final abstract class DatabaseNamesMetaTypes extends MetaTypes {
  override type ResourceNameScope = DatabaseMetaTypes#ResourceNameScope
  override type ColumnType = DatabaseMetaTypes#ColumnType
  override type ColumnValue = DatabaseMetaTypes#ColumnValue
  override type DatabaseTableNameImpl = String
  override type DatabaseColumnNameImpl = String
}

object DatabaseNamesMetaTypes extends MetaTypeHelper[DatabaseNamesMetaTypes] {
  def rewriteFrom(analysis: SoQLAnalysis[DatabaseMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseNamesMetaTypes](
      { case DatabaseTableName(dtn) => DatabaseTableName(dtn.dataTableName) },
      { case (dtn, DatabaseColumnName(columnInfo)) => DatabaseColumnName(columnInfo.physicalColumnBase) }
    )
  }

  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
  }
}
