package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._

case class AugmentedType[MT <: MetaTypes](typ: MT#ColumnType, isExpanded: Boolean)
