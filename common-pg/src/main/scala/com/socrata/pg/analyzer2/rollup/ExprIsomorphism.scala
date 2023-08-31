package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait ExprIsomorphism[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def isIsomorphicTo(e1: Expr, e2: Expr): Boolean
  def isIsomorphicTo(e1: Option[Expr], e2: Option[Expr]): Boolean =
    (e1, e2) match {
      case (None, None) => true
      case (Some(a), Some(b)) => isIsomorphicTo(a, b)
      case _ => false
    }
}
