package com.socrata.pg.soql

import com.socrata.soql.SoQLAnalysis
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.types.SoQLType

class SoQLAnalysisSqlizer(analysis: SoQLAnalysis[UserColumnId, SoQLType]) extends Sqlizer[SoQLAnalysis[UserColumnId, SoQLType]] {

  import Sqlizer._

  // TODO: Search
  def sql = {
    analysis.selection.map { case (columnName, coreExpr) => coreExpr.sql}.mkString("SELECT ", ",", "") +
      analysis.where.map(" WHERE " +  _.sql).getOrElse("") +
      analysis.groupBy.map(_.map(_.sql).mkString(" GROUP BY ", ",", "")).getOrElse("") +
      analysis.having.map(" HAVING " + _.sql).getOrElse("") +
      analysis.orderBy.map(_.map(_.sql).mkString(" ORDER BY ", ",", "")).getOrElse("") +
      analysis.limit.map(" LIMIT " + _.toString).getOrElse("") +
      analysis.offset.map(" OFFSET " + _.toString).getOrElse("")
  }
}