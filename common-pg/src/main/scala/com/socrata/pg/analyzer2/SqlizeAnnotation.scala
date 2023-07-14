package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName

sealed abstract class SqlizeAnnotation[MT <: MetaTypes] extends SqlizerUniverse[MT]

object SqlizeAnnotation {
  case class Expression[MT <: MetaTypes](expr: Expr[MT]) extends SqlizeAnnotation[MT]
  case class Table[MT <: MetaTypes](table: AutoTableLabel) extends SqlizeAnnotation[MT]
  case class OutputName[MT <: MetaTypes](name: ColumnName) extends SqlizeAnnotation[MT]
}
