package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.pg.analyzer2.Stringifier

class SoQLRollupExact[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})](
  stringifier: Stringifier[MT]
) extends RollupExact[MT](new SoQLSemigroupRewriter[MT], new SoQLFunctionSubset[MT], new SoQLFunctionSplitter[MT], new SoQLSplitAnd[MT], stringifier)
