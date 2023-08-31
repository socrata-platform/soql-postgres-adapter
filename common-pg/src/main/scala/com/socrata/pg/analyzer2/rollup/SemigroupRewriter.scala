package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait SemigroupRewriter[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  // This is responsible for doing things that look like
  //   count(x) => coalesce(sum(count_x), 0)
  //   max(x) => max(max_x)
  def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr]
}
