package com.socrata.pg

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.Indexable
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLAnalysisType, SoQLValue, SoQLType}

package object soql {
  type AnalysisTarget = (AUserCol, String, Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type ASysCol = Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]]
  type AUserCol = Seq[SoQLAnalysis[UserColumnId, SoQLType]]
  type SqlCol = SqlColumnRep[SoQLType, SoQLValue]
  type SqlColIdx = SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]
  type Escape = String => String
}
