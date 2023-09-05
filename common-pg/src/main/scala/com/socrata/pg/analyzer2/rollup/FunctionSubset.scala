package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait FunctionSubset[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  // returns true if a call to A with the same args as a call to B can
  // be considered a "subset of columns".
  // e.g., date_trunc_ym(some_date) is a "subset of columns" of date_trunc_ymd(some_date)
  // and as a result, count(date_trunc_ym(some_date)) can be rewritten to use
  // count(date_trunc_ymd(some_date)) by (approximately) rewriting it to
  // sum(date_trunc_ymd_some_date)
  def functionSubset(a: MonomorphicFunction, b: MonomorphicFunction): Boolean
}
