package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import java.sql.PreparedStatement

import Sqlizer._

class StringLiteralSqlizer(lit: StringLiteral[SoQLType]) extends Sqlizer[StringLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) = {
    val s = "?" //  s"'${lit.value.replace("'", "''")}'"
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      stmt.foreach(_.setString(pos, lit.value))
      Some(lit.value)
    }
    ParametricSql(s, setParams :+ setParam)
  }
}

class NumberLiteralSqlizer(lit: NumberLiteral[SoQLType]) extends Sqlizer[NumberLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) = {
    val s = "?" // s"${lit.value}"
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      stmt.foreach(_.setBigDecimal(pos, lit.value.bigDecimal))
      Some(lit.value)
    }
    ParametricSql(s, setParams :+ setParam)
  }
}

class BooleanLiteralSqlizer(lit: BooleanLiteral[SoQLType]) extends Sqlizer[BooleanLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) = {
    val s = "?"  // s"'${lit.value.toString}'"
    val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
      stmt.foreach(_.setBoolean(pos, lit.value))
      Some(lit.value)
    }
    ParametricSql(s, setParams :+ setParam)
  }
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) =
    ParametricSql("null", setParams)
}

class FunctionCallSqlizer(expr: FunctionCall[UserColumnId, SoQLType]) extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) = {
    val fn = SqlFunctions(expr.function.function)
    fn(expr, rep, setParams, idRep, verRep)
  }
}

class ColumnRefSqlizer(expr: ColumnRef[UserColumnId, SoQLType]) extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  def sql(reps: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep) = {
    reps.get(expr.column) match {
      case Some(rep) => ParametricSql(rep.physColumns.mkString(","), setParams)
      case None => ParametricSql(expr.column.underlying, setParams) // for tests
    }
  }
}