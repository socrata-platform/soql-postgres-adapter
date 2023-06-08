package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._

sealed abstract class SqlizeAnnotation[MT <: MetaTypes] extends SqlizerUniverse[MT]

object SqlizeAnnotation {
  case class Expression[MT <: MetaTypes](expr: Expr[MT]) extends SqlizeAnnotation[MT]
  case class Table[MT <: MetaTypes](table: AutoTableLabel) extends SqlizeAnnotation[MT]
}
