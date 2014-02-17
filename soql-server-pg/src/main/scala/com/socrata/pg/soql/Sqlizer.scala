package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.typed._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.SoQLAnalysis
import com.socrata.datacoordinator.truth.sql.SqlColumnRep


trait Sqlizer[T] {
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]): String
}

object Sqlizer {
  implicit def stringLiteralSqlizer(lit: StringLiteral[SoQLType]): Sqlizer[StringLiteral[SoQLType]] = {
    new StringLiteralSqlizer(lit)
  }

  implicit def functionCallSqlizer(lit: FunctionCall[UserColumnId, SoQLType]): Sqlizer[FunctionCall[UserColumnId, SoQLType]] = {
    new FunctionCallSqlizer(lit)
  }

  implicit def coreExprSqlizer(expr: CoreExpr[UserColumnId, SoQLType]): Sqlizer[_] = {
    expr match {
      case fc: FunctionCall[UserColumnId, SoQLType] => new FunctionCallSqlizer(fc)
      case cr: ColumnRef[UserColumnId, SoQLType] => new ColumnRefSqlizer(cr)
      case lit: StringLiteral[SoQLType] => new StringLiteralSqlizer(lit)
      case lit: NumberLiteral[SoQLType] => new NumberLiteralSqlizer(lit)
      case lit: BooleanLiteral[SoQLType] => new BooleanLiteralSqlizer(lit)
      case lit: NullLiteral[SoQLType] => NullLiteralSqlizer
    }
  }

  implicit def orderBySqlizer(ob: OrderBy[UserColumnId, SoQLType]): Sqlizer[OrderBy[UserColumnId, SoQLType]] = {
    new OrderBySqlizer(ob)
  }

  implicit def analysisSqlizer(analysisTable: Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]): Sqlizer[Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]] = {
    new SoQLAnalysisSqlizer(analysisTable._1, analysisTable._2)
  }
}
