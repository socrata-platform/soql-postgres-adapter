package com.socrata.pg

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.Indexable
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ColumnName, TableRef, Qualified, ResourceName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

package object soql {
  type AnalysisTarget = (AUserCol, Map[ResourceName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type AUserCol = (NonEmptySeq[SoQLAnalysis[Qualified[UserColumnId], SoQLType]], Option[String])
  type TopAnalysisTarget = (TopAUserCol, ResourceName, Map[ResourceName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type TopAUserCol = NonEmptySeq[SoQLAnalysis[Qualified[UserColumnId], SoQLType]]
  type ASysCol = NonEmptySeq[SoQLAnalysis[Qualified[ColumnName], SoQLType]]
  type SqlCol = SqlColumnRep[SoQLType, SoQLValue]
  type SqlColIdx = SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]
  type Escape = String => String
}
