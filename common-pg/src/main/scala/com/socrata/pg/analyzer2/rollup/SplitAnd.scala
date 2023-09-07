package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait SplitAnd[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def split(e: Expr): Seq[Expr]
  def merge(es: Seq[Expr]): Option[Expr]
}
