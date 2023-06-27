package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

sealed abstract class ExprSql[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def compressed: ExprSql.Compressed[MT]
  def expr: Expr
  def typ: CT = expr.typ
  def sqls: Seq[Doc]
  def databaseExprCount: Int
  def isExpanded: Boolean
  def withExpr(newExpr: Expr): ExprSql
}
object ExprSql {
  def apply[MT <: MetaTypes](sql: Doc[SqlizeAnnotation[MT]], expr: Expr[MT]): Compressed[MT] =
    new Compressed(sql, expr)

  def apply[MT <: MetaTypes](sqls: Seq[Doc[SqlizeAnnotation[MT]]], expr: Expr[MT])(implicit repFor: Rep.Provider[MT], gensymProvider: GensymProvider): ExprSql[MT] =
    if(sqls.lengthCompare(1) == 0) {
      new Compressed(sqls.head, expr)
    } else {
      new Expanded(sqls, expr)
    }

  class Compressed[MT <: MetaTypes] private[ExprSql] (rawSql: Doc[SqlizeAnnotation[MT]], val expr: Expr[MT]) extends ExprSql[MT] {
    def compressed = this
    def sql = rawSql.annotate(SqlizeAnnotation.Expression(expr))
    def sqls = Seq(sql)
    def databaseExprCount = 1
    def isExpanded = false
    def withExpr(newExpr: Expr) = new Compressed(rawSql, newExpr)
  }

  class Expanded[MT <: MetaTypes] private[ExprSql] (rawSqls: Seq[Doc[SqlizeAnnotation[MT]]], val expr: Expr[MT])(implicit repFor: Rep.Provider[MT], gensymProvider: GensymProvider) extends ExprSql[MT] {
    assert(rawSqls.lengthCompare(1) != 0)

    def sqls = rawSqls.map(_.annotate(SqlizeAnnotation.Expression(expr)))

    def compressed = {
      val compressedSql =
        expr match {
          case _ : NullLiteral =>
            d"null :: jsonb"
          case _ =>
            rawSqls.funcall(d"soql_compress_compound")
        }

      ExprSql(
        compressedSql,
        expr
      )
    }

    def databaseExprCount = rawSqls.length

    def isExpanded = true

    def withExpr(newExpr: Expr) = new Expanded(rawSqls, newExpr)
  }

  object Expanded {
    def apply[MT <: MetaTypes](sqls: Seq[Doc[SqlizeAnnotation[MT]]], expr: Expr[MT])(implicit repFor: Rep.Provider[MT], gensymProvider: GensymProvider): Expanded[MT] =
      new Expanded(sqls.map(_.annotate(SqlizeAnnotation.Expression(expr))), expr)
  }
}
