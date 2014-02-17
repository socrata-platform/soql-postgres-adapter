package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

class StringLiteralSqlizer(lit: StringLiteral[SoQLType]) extends Sqlizer[StringLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = s"'${lit.value.replace("'", "''")}'"
}

class NumberLiteralSqlizer(lit: NumberLiteral[SoQLType]) extends Sqlizer[NumberLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = s"${lit.value}"
}

class BooleanLiteralSqlizer(lit: BooleanLiteral[SoQLType]) extends Sqlizer[BooleanLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = s"'${lit.value.toString}'"
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = "null"
}

class FunctionCallSqlizer(expr: FunctionCall[UserColumnId, SoQLType]) extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = {
    val fn = SqlFunctions(expr.function.name).get
    fn(expr, rep).toString
  }
}

class ColumnRefSqlizer(expr: ColumnRef[UserColumnId, SoQLType]) extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  def sql(reps: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = {
    reps.get(expr.column) match {
      case Some(rep) => rep.physColumns.mkString(",")
      case None => expr.column.underlying // for tests
    }
  }
}