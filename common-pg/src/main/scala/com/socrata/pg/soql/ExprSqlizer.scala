package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import java.sql.PreparedStatement
import Sqlizer._
import SqlizerContext._

object StringLiteralSqlizer extends Sqlizer[StringLiteral[SoQLType]] {

  def sql(lit: StringLiteral[SoQLType])
         (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        val v = toUpper(lit, quote(lit.value, escape), ctx)
        ParametricSql(v, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        val v = toUpper(lit, quote(lit.value, escape), ctx)
        ParametricSql(v, setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          val maybeUpperLitVal = toUpper(lit, lit.value, ctx)
          stmt.foreach(_.setString(pos, maybeUpperLitVal))
          Some(maybeUpperLitVal)
        }
        ParametricSql(ParamPlaceHolder, setParams :+ setParam)
    }
  }

  private def quote(s: String, escape: Escape) = {
    s"e'${escape(s)}'"
  }

  private def toUpper(lit: StringLiteral[SoQLType], v: String, ctx: Context): String =
    if (useUpper(lit)(ctx)) v.toUpperCase else v
}

object NumberLiteralSqlizer extends Sqlizer[NumberLiteral[SoQLType]] {

  def sql(lit: NumberLiteral[SoQLType])
         (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(lit.value.bigDecimal.toPlainString, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        ParametricSql(lit.value.bigDecimal.toPlainString, setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          stmt.foreach(_.setBigDecimal(pos, lit.value.bigDecimal))
          Some(lit.value)
        }
        ParametricSql(ParamPlaceHolder, setParams :+ setParam)
    }
  }
}

object BooleanLiteralSqlizer extends Sqlizer[BooleanLiteral[SoQLType]] {

  def sql(lit: BooleanLiteral[SoQLType])
         (rep: Map[UserColumnId,
          SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(lit.toString, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        ParametricSql(lit.toString, setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          stmt.foreach(_.setBoolean(pos, lit.value))
          Some(lit.value)
        }
        ParametricSql(ParamPlaceHolder, setParams :+ setParam)
    }
  }
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {

  def sql(lit: NullLiteral[SoQLType])
         (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) =
    ParametricSql("null", setParams)
}

object FunctionCallSqlizer extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {


  def sql(expr: FunctionCall[UserColumnId, SoQLType])
         (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) = {
    val fn = SqlFunctions(expr.function.function)
    val ParametricSql(sql, fnSetParams) = fn(expr, rep, setParams, ctx, escape)
    // SoQL parsing bakes parenthesis into the ast tree without explicitly spitting out parenthesis.
    // We add parenthesis to every function call to preserve semantics.
    ParametricSql(s"($sql)", fnSetParams)
  }
}

object ColumnRefSqlizer extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  def sql(expr: ColumnRef[UserColumnId, SoQLType])
         (reps: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape) = {
    reps.get(expr.column) match {
      case Some(rep) =>
        val maybeUpperPhysColumns = rep.physColumns.map(toUpper(expr)(_, ctx))
        ParametricSql(maybeUpperPhysColumns.mkString(","), setParams)
      case None =>
        ParametricSql(toUpper(expr)(expr.column.underlying, ctx), setParams) // for tests
    }
  }

  private def toUpper(expr: ColumnRef[UserColumnId, SoQLType])(phyColumn: String, ctx: Context): String =
    if (expr.typ == SoQLText && useUpper(expr)(ctx) ) s"upper($phyColumn)"
    else phyColumn
}