package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import java.sql.PreparedStatement
import Sqlizer._
import SqlizerContext._

class StringLiteralSqlizer(lit: StringLiteral[SoQLType]) extends Sqlizer[StringLiteral[SoQLType]] {

  val underlying = lit

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        val v = toUpper(quote(lit.value), ctx)
        ParametricSql(v, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(ctx) =>
        val v = toUpper(quote(lit.value), ctx)
        ParametricSql(v, setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          val maybeUpperLitVal = toUpper(lit.value, ctx)
          stmt.foreach(_.setString(pos, maybeUpperLitVal))
          Some(maybeUpperLitVal)
        }
        ParametricSql(ParamPlaceHolder, setParams :+ setParam)
    }
  }

  private def quote(s: String) = "'" + s.replaceAllLiterally("'", "''") + "'"

  private def toUpper(lit: String, ctx: Context): String = if (useUpper(ctx)) lit.toUpperCase else lit
}

class NumberLiteralSqlizer(lit: NumberLiteral[SoQLType]) extends Sqlizer[NumberLiteral[SoQLType]] {

  val underlying = lit

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(lit.value.bigDecimal.toPlainString, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(ctx) =>
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

class BooleanLiteralSqlizer(lit: BooleanLiteral[SoQLType]) extends Sqlizer[BooleanLiteral[SoQLType]] {

  val underlying = lit

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {

    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(lit.toString, setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(ctx) =>
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

class NullLiteralSqlizer(lit: NullLiteral[SoQLType]) extends Sqlizer[NullLiteral[SoQLType]] {

  val underlying = lit

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) =
    ParametricSql("null", setParams)
}

class FunctionCallSqlizer(expr: FunctionCall[UserColumnId, SoQLType]) extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {

  val underlying = expr

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    val fn = SqlFunctions(expr.function.function)
    val ParametricSql(sql, fnSetParams) = fn(expr, rep, setParams, ctx)
    // SoQL parsing bakes parenthesis into the ast tree without explicitly spitting out parenthesis.
    // We add parenthesis to every function call to preserve semantics.
    ParametricSql(s"($sql)", fnSetParams)
  }
}

class ColumnRefSqlizer(expr: ColumnRef[UserColumnId, SoQLType]) extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  val underlying = expr

  def sql(reps: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    reps.get(expr.column) match {
      case Some(rep) =>
        val maybeUpperPhysColumns = rep.physColumns.map(toUpper(_, ctx))
        val columnsWithGeoSqlized = maybeUpperPhysColumns.map(sqlizeGeoColumnRef(_, ctx))
        ParametricSql(columnsWithGeoSqlized.mkString(","), setParams)
      case None => {
        ParametricSql(sqlizeGeoColumnRef(toUpper(expr.column.underlying, ctx), ctx), setParams) // for tests
      }
    }
  }

  private def toUpper(phyColumn: String, ctx: Context): String =
    if (expr.typ == SoQLText && useUpper(ctx) ) s"upper($phyColumn)"
    else phyColumn

  private def sqlizeGeoColumnRef(phyColumn: String, ctx: Context): String = {
    if (isGeoColumn && ctx.get(SoqlPart) == Some(SoqlSelect)) {
      s"ST_AsGeoJson($phyColumn)"
    }
    else {
      phyColumn
    }
  }

  private def isGeoColumn = expr.typ == SoQLPoint || expr.typ == SoQLLine || expr.typ == SoQLPolygon
}