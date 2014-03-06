package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.typed.{StringLiteral, OrderBy, CoreExpr}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.pg.store.PostgresUniverseCommon
import scala.util.parsing.input.NoPosition

class SoQLAnalysisSqlizer(analysis: SoQLAnalysis[UserColumnId, SoQLType], tableName: String, allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]])
      extends Sqlizer[Tuple3[SoQLAnalysis[UserColumnId, SoQLType], String, Seq[SqlColumnRep[SoQLType, SoQLValue]]]] {

  import Sqlizer._
  import SqlizerContext._

  val underlying = Tuple3(analysis, tableName, allColumnReps)

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context) = {

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect) + (Analysis -> analysis)
    val (selectPhrase, setParamsSelect) = analysis.selection.foldLeft(Tuple2(Seq.empty[String], setParams)) { (t2, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val ParametricSql(sql, newSetParams) = coreExpr.sql(rep, t2._2, ctxSelect + (CurrentSelectedColumn -> coreExpr))
      (t2._1 :+ sql, newSetParams)
    }

    // WHERE
    val where = analysis.where.map(_.sql(rep, setParamsSelect, ctx + (SoqlPart -> SoqlWhere)))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelect)

    // SEARCH
    val search = analysis.search.map { search =>
      val searchLit = StringLiteral(search, SoQLText)(NoPosition)
      val ParametricSql(searchSql, searchSetParams) = searchLit.sql(rep, setParamsWhere, ctx + (SoqlPart -> SoqlSearch))

      PostgresUniverseCommon.searchVector(allColumnReps) match {
        case Some(sv) =>
          val andOrWhere = if (where.isDefined) " AND" else " WHERE"
          val fts = s"$andOrWhere $sv @@ plainto_tsquery($searchSql)"
          ParametricSql(fts, searchSetParams)
        case None =>
          ParametricSql("", setParamsWhere)
      }
    }
    val setParamsSearch = search.map(_.setParams).getOrElse(setParamsWhere)

    // GROUP BY
    val groupBy = analysis.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsSearch)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
      val ParametricSql(sql, newSetParams) = gb.sql(rep, t2._2, ctx + (SoqlPart -> SoqlGroup))
      (t2._1 :+ sql, newSetParams)
    }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsSearch)

    // HAVING
    val having = analysis.having.map(_.sql(rep, setParamsGroupBy, ctx + (SoqlPart -> SoqlHaving)))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = analysis.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val ParametricSql(sql, newSetParams) = ob.sql(rep, t2._2, ctx + (SoqlPart -> SoqlOrder))
        (t2._1 :+ sql, newSetParams)
      }}
    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    // COMPLETE SQL
    val select = selectPhrase.mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      where.map(" WHERE " +  _.sql).getOrElse("") +
      search.map(_.sql).getOrElse("") +
      groupBy.map(_._1.mkString(" GROUP BY ", ",", "")).getOrElse("") +
      having.map(" HAVING " +  _.sql).getOrElse("") +
      orderBy.map(_._1.mkString(" ORDER BY ", ",", "")).getOrElse("") +
      analysis.limit.map(" LIMIT " + _.toString).getOrElse("") +
      analysis.offset.map(" OFFSET " + _.toString).getOrElse("")
    ParametricSql(select, setParamsOrderBy)
  }
}