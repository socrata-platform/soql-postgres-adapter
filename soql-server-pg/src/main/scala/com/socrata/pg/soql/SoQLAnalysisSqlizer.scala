package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.typed.{OrderBy, CoreExpr}

class SoQLAnalysisSqlizer(analysis: SoQLAnalysis[UserColumnId, SoQLType], tableName: String) extends Sqlizer[Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]] {

  import Sqlizer._

  // TODO: Search
  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]) = {

    // SELECT
    val (selectPhrase, setParamsSelect) = analysis.selection.foldLeft(Tuple2(Seq.empty[String], setParams)) { (t2, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val ParametricSql(sql, newSetParams) = coreExpr.sql(rep, t2._2)
      (t2._1 :+ sql, newSetParams)
    }

    // WHERE
    val where = analysis.where.map(_.sql(rep, setParamsSelect))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelect)

    // GROUP BY
    val groupBy = analysis.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsWhere)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
      val ParametricSql(sql, newSetParams) = gb.sql(rep, t2._2)
      (t2._1 :+ sql, newSetParams)
    }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsWhere)

    // HAVING
    val having = analysis.having.map(_.sql(rep, setParamsGroupBy))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = analysis.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val ParametricSql(sql, newSetParams) = ob.sql(rep, t2._2)
        (t2._1 :+ sql, newSetParams)
      }}
    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    // COMPLETE SQL
    val select = selectPhrase.mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      where.map(" WHERE " +  _.sql).getOrElse("") +
      groupBy.map(_._1.mkString(" GROUP BY ", ",", "")).getOrElse("") +
      having.map(" HAVING " +  _.sql).getOrElse("") +
      orderBy.map(_._1.mkString(" ORDER BY ", ",", "")).getOrElse("") +
      analysis.limit.map(" LIMIT " + _.toString).getOrElse("") +
      analysis.offset.map(" OFFSET " + _.toString).getOrElse("")
    ParametricSql(select, setParamsOrderBy)
  }
}