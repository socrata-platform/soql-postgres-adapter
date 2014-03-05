package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types.{SoQLValue, SoQLType, SoQLText}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import java.sql.PreparedStatement

import Sqlizer._

class StringLiteralSqlizer(lit: StringLiteral[SoQLType]) extends Sqlizer[StringLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      val maybeUpperLitVal = toUpper(lit.value, ctx)
      stmt.foreach(_.setString(pos, maybeUpperLitVal))
      Some(maybeUpperLitVal)
    }
    ParametricSql(ParamPlaceHolder, setParams :+ setParam)
  }

  private def toUpper(lit: String, ctx: Context): String = if (useUpper(ctx)) lit.toUpperCase else lit
}

class NumberLiteralSqlizer(lit: NumberLiteral[SoQLType]) extends Sqlizer[NumberLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      stmt.foreach(_.setBigDecimal(pos, lit.value.bigDecimal))
      Some(lit.value)
    }
    ParametricSql(ParamPlaceHolder, setParams :+ setParam)
  }
}

class BooleanLiteralSqlizer(lit: BooleanLiteral[SoQLType]) extends Sqlizer[BooleanLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      stmt.foreach(_.setBoolean(pos, lit.value))
      Some(lit.value)
    }
    ParametricSql(ParamPlaceHolder, setParams :+ setParam)
  }
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) =
    ParametricSql("null", setParams)
}

class FunctionCallSqlizer(expr: FunctionCall[UserColumnId, SoQLType]) extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    val fn = SqlFunctions(expr.function.function)
    val ParametricSql(sql, fnSetParams) = fn(expr, rep, setParams, ctx)
    // SoQL parsing bakes parenthesis into the ast tree without explicitly spitting out parenthesis.
    // We add parenthesis to every function call to preserve semantics.
    ParametricSql(s"($sql)", fnSetParams)
  }
}

class ColumnRefSqlizer(expr: ColumnRef[UserColumnId, SoQLType]) extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  def sql(reps: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {
    reps.get(expr.column) match {
      case Some(rep) =>
        val maybeUpperPhysColumns = rep.physColumns.map(toUpper(_, ctx))
        ParametricSql(maybeUpperPhysColumns.mkString(","), setParams)
      case None =>
        ParametricSql(toUpper(expr.column.underlying, ctx), setParams) // for tests
    }
  }

  private def toUpper(phyColumn: String, ctx: Context): String =
    if (expr.typ == SoQLText && useUpper(ctx) ) s"upper($phyColumn)"
    else phyColumn
}