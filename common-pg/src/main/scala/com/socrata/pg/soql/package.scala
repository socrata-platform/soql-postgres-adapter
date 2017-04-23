package com.socrata.pg

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.Indexable
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

package object soql {
  type AnalysisTarget = (AUserCol, Map[TableName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type ASysCol = Seq[SoQLAnalysis[ColumnName, SoQLType]]
  type AUserCol = Seq[SoQLAnalysis[UserColumnId, SoQLType]]
  type SqlCol = SqlColumnRep[SoQLType, SoQLValue]
  type SqlColIdx = SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]
  type Escape = String => String
}
