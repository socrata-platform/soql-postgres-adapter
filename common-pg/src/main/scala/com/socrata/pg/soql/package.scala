package com.socrata.pg

import com.socrata.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.Indexable
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

package object soql {
  type AnalysisTarget = (AUserCol, Map[TableName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type AUserCol = NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]]
  type ASysCol = NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]
  type SqlCol = SqlColumnRep[SoQLType, SoQLValue]
  type SqlColIdx = SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]

  type BAnalysisTarget = (BUserCol, Map[TableName, String], Seq[SqlColumnRep[SoQLType, SoQLValue]])
  type BUserCol = BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]
  type BSysCol = BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]
  type Escape = String => String
}
