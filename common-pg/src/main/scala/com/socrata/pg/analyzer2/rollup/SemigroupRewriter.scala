package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.pg.analyzer2.SqlizerUniverse

trait SemigroupRewriter[MT <: MetaTypes]
    extends (MonomorphicFunction[MT#ColumnType] => Option[Expr[MT] => Expr[MT]])
    with SqlizerUniverse[MT]
{
  // This is responsible for doing things that look like
  //   count(x) => coalesce(sum(count_x), 0)
  //   max(x) => max(max_x)
  override def apply(f: MonomorphicFunction): Option[Expr => Expr]
}
