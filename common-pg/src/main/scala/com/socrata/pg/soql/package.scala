package com.socrata.pg

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.index.Indexable
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ColumnName, TableRef, Qualified, ResourceName}
import com.socrata.soql.types.{SoQLType, SoQLValue}

package object soql {
  type SqlCol = SqlColumnRep[SoQLType, SoQLValue]
  type SqlColIdx = SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]
  type Escape = String => String
}
