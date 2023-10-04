package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.pg.analyzer2.SqlizerUniverse

trait FunctionSplitter[MT <: MetaTypes]
    extends (MonomorphicFunction[MT#ColumnType] => Option[(MonomorphicFunction[MT#ColumnType], Seq[MonomorphicFunction[MT#ColumnType]])])
    with SqlizerUniverse[MT]
{
  // This is responsible for doing things that look like
  //   avg(x) => sum(x) / count(x)
  override def apply(f: MonomorphicFunction): Option[(MonomorphicFunction, Seq[MonomorphicFunction])]
}
