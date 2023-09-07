package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait Stringifier[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def statement(stmt: Statement): LazyToString
  def from(from: From): LazyToString
  def expr(expr: Expr): LazyToString
}

object Stringifier {
  def simple[MT <: MetaTypes]: Stringifier[MT] =
    new Stringifier[MT] {
      override def statement(stmt: Statement) = LazyToString(stmt)(_.toString)
      override def from(from: From) = LazyToString(from)(_.toString)
      override def expr(expr: Expr) = LazyToString(expr)(_.toString)
    }

  def pretty[MT <: MetaTypes](
    implicit cvDoc: HasDoc[MT#ColumnValue],
    dtnDoc: HasDoc[MT#DatabaseTableNameImpl],
    dcnDoc: HasDoc[MT#DatabaseColumnNameImpl]
  ): Stringifier[MT] =
    new Stringifier[MT] {
      override def statement(stmt: Statement) = LazyToString(stmt)(_.debugStr)
      override def from(from: From) = LazyToString(from)(_.debugStr)
      override def expr(expr: Expr) = LazyToString(expr)(_.debugStr)
    }
}
