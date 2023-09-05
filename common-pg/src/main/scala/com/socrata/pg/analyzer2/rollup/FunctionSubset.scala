package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait FunctionSubset[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def funcallSubset(a: Expr, b: Expr, under: IsomorphismState.View[MT]): Option[Expr => Expr]
}

trait SimpleFunctionSubset[MT <: MetaTypes] extends FunctionSubset[MT] {
  // If a call to A can be rewritten to some extraction of a call to
  // B, then this produces a function which does that rewrite on the
  // result of an extraction of a call to B.
  //
  // e.g., date_trunc_ym(some_date) is a "subset of columns" of date_trunc_ymd(some_date)
  // and as a result, count(date_trunc_ym(some_date)) can be rewritten to use
  // count(date_trunc_ymd(some_date)) by (approximately) rewriting it to
  // sum(date_trunc_ymd_some_date)
  protected def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction): Option[MonomorphicFunction]

  // Helper function that does this same comparison but on concrete
  // expressions.  This can be overridden for more complex behavior
  // than single-arg composition.
  //
  // e.g., if you have a query containing the expression
  //    date_trunc_ym(some_fixed_timestamp, "some_timezone")
  // and a rollup selecting the expression
  //    date_trunc_ymd(some_fixed_timestamp, "some_timezone") as x
  // then that query expression can be rewritten as
  //    date_trunc_ym(the_rollup.x)
  // but this implementation will not do that.  You'll need to
  // override it if it's wanted.
  override def funcallSubset(a: Expr, b: Expr, under: IsomorphismState.View[MT]): Option[Expr => Expr] =
    (a, b) match {
      case (aFC@FunctionCall(aFunc, Seq(aArg)), FunctionCall(bFunc, Seq(bArg))) =>
        funcallSubset(aFunc, bFunc) match {
          case Some(newFunc) if aArg.isIsomorphic(bArg, under) =>
            Some { expr => FunctionCall(newFunc, Seq(expr))(aFC.position) }
          case None =>
            None
        }
      case _ =>
        None
    }
}
