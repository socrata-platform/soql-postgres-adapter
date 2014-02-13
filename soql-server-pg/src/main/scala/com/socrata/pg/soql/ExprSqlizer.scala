package com.socrata.pg.soql

import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType
import com.socrata.datacoordinator.id.UserColumnId

class StringLiteralSqlizer(lit: StringLiteral[SoQLType]) extends Sqlizer[StringLiteral[SoQLType]] {
  def sql = s"'${lit.value.replace("'", "''")}'"
}

class NumberLiteralSqlizer(lit: NumberLiteral[SoQLType]) extends Sqlizer[NumberLiteral[SoQLType]] {
  def sql = s"${lit.value}"
}

class BooleanLiteralSqlizer(lit: BooleanLiteral[SoQLType]) extends Sqlizer[BooleanLiteral[SoQLType]] {
  def sql = s"'${lit.value.toString}'"
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {
  def sql = "null"
}

class FunctionCallSqlizer(expr: FunctionCall[UserColumnId, SoQLType]) extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {

  def sql = {
    val fn = SqlFunctions(expr.function.name).get
    fn(expr).toString
  }
}

class ColumnRefSqlizer(expr: ColumnRef[UserColumnId, SoQLType]) extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {
  def sql = expr.column.underlying
}