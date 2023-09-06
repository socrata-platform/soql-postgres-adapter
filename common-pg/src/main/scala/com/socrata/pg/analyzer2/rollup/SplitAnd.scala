package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait SplitAnd[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def splitAnd(e: Expr): Seq[Expr]
  def mergeAnd(es: Seq[Expr]): Option[Expr]
}
