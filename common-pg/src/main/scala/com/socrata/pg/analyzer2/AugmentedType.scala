package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._

case class AugmentedType[MT <: MetaTypes](rep: Rep[MT], isExpanded: Boolean) {
  def typ = rep.typ
}
