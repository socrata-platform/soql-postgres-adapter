package com.socrata.pg.soql

import com.socrata.soql.SoQLAnalysis
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

class SoQLAnalysisSqlizer(analysis: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) extends Sqlizer[Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]] {

  import Sqlizer._

  // TODO: Search
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = {
    analysis.selection.map { case (columnName, coreExpr) => coreExpr.sql(rep) }.mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      analysis.where.map(" WHERE " +  _.sql(rep)).getOrElse("") +
      analysis.groupBy.map(_.map(_.sql(rep)).mkString(" GROUP BY ", ",", "")).getOrElse("") +
      analysis.having.map(" HAVING " + _.sql(rep)).getOrElse("") +
      analysis.orderBy.map(_.map(_.sql(rep)).mkString(" ORDER BY ", ",", "")).getOrElse("") +
      analysis.limit.map(" LIMIT " + _.toString).getOrElse("") +
      analysis.offset.map(" OFFSET " + _.toString).getOrElse("")
  }
}