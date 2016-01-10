package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.soql.typed.{StringLiteral, OrderBy, CoreExpr}
import scala.util.parsing.input.NoPosition

// scalastyle:off import.grouping
object SoQLAnalysisSqlizer extends Sqlizer[AnalysisTarget] {
  import Sqlizer._
  import SqlizerContext._

  def sql(analysis: AnalysisTarget)
         (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    sql(analysis, reqRowCount = false, rep, setParams, ctx, escape)
  }

  /**
   * For rowcount w/o group by, just replace the select with count(*).
   * For rowcount with group by, wrap the original group by sql with a select count(*) from ( {original}) t_rc
   */
  def rowCountSql(analysis: AnalysisTarget)
                 (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  ctx: Context,
                  escape: Escape): ParametricSql = {
    sql(analysis, reqRowCount = true, rep, setParams, ctx, escape)
  }

  /**
   * Convert chained analyses to parameteric sql.
   */
  private def sql(analysisTarget: AnalysisTarget, // scalastyle:ignore method.length
                  reqRowCount: Boolean,
                  rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  context: Context,
                  escape: Escape): ParametricSql = {
    val (analyses, tableName, allColumnReps) = analysisTarget
    val ctx = context + (Analysis -> analyses)
    val firstAna = analyses.head
    val lastAna = analyses.last
    val rowCountForFirstAna = reqRowCount && (firstAna == lastAna)
    val firstCtx = ctx + (OutermostSoql -> outermostSoql(firstAna, analyses)) +
                         (InnermostSoql -> innermostSoql(firstAna, analyses))
    val firstSql = sql(firstAna, tableName, allColumnReps, rowCountForFirstAna, rep, setParams, firstCtx, escape)
    var subTableIdx = 0
    val (result, _) = analyses.drop(1).foldLeft((firstSql, analyses.head)) { (acc, ana) =>
      subTableIdx += 1
      val (subParametricSql, prevAna) = acc
      val subTableName = "(%s) AS x%d".format(subParametricSql.sql.head, subTableIdx)
      val subCtx = ctx + (OutermostSoql -> outermostSoql(ana, analyses)) +
                         (InnermostSoql -> innermostSoql(ana, analyses))
      val sqls = sql(ana, subTableName, allColumnReps, reqRowCount &&  ana == lastAna,
          rep, subParametricSql.setParams, subCtx, escape)
      (sqls, ana)
    }
    result
  }

  /**
   * Convert one analysis to parameteric sql.
   */
  private def sql(analysis: SoQLAnalysis[UserColumnId, SoQLType], // scalastyle:ignore method.length
                  tableName: String,
                  allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
                  reqRowCount: Boolean,
                  rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  context: Context,
                  escape: Escape): ParametricSql = {
    val ana = if (reqRowCount) rowCountAnalysis(analysis) else analysis
    val ctx = context + (Analysis -> analysis)

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)
    val (selectPhrase, setParamsSelect) =
      if (reqRowCount && analysis.groupBy.isEmpty) {
        (Seq("count(*)"), setParams)
      } else {
        select(analysis)(rep, setParams, ctxSelect, escape)
      }

    // WHERE
    val where = ana.where.map(Sqlizer.sql(_)(rep, setParamsSelect, ctx + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelect)

    // SEARCH
    val search = ana.search.map { search =>
      val searchLit = StringLiteral[SoQLType](search, SoQLText)(NoPosition)
      val ParametricSql(Seq(searchSql), searchSetParams) =
        Sqlizer.sql(searchLit)(rep, setParamsWhere, ctx + (SoqlPart -> SoqlSearch), escape)

      PostgresUniverseCommon.searchVector(allColumnReps) match {
        case Some(sv) =>
          val andOrWhere = if (where.isDefined) " AND" else " WHERE"
          val fts = s"$andOrWhere $sv @@ plainto_tsquery('english', $searchSql)"
          ParametricSql(Seq(fts), searchSetParams)
        case None =>
          ParametricSql(Seq(""), setParamsWhere)
      }
    }
    val setParamsSearch = search.map(_.setParams).getOrElse(setParamsWhere)

    // GROUP BY
    val groupBy = ana.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsSearch)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
        val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(gb)(rep, t2._2, ctx + (SoqlPart -> SoqlGroup), escape)
        (t2._1 :+ sql, newSetParams)
      }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsSearch)

    // HAVING
    val having = ana.having.map(Sqlizer.sql(_)(rep, setParamsGroupBy, ctx + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = ana.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val ParametricSql(Seq(sql), newSetParams) =
          Sqlizer.sql(ob)(rep, t2._2, ctx + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
        (t2._1 :+ sql, newSetParams)
      }}
    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    // COMPLETE SQL
    val completeSql = selectPhrase.mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      where.flatMap(_.sql.headOption.map(" WHERE " +  _)).getOrElse("") +
      search.flatMap(_.sql.headOption).getOrElse("") +
      groupBy.map(_._1.mkString(" GROUP BY ", ",", "")).getOrElse("") +
      having.flatMap(_.sql.headOption.map(" HAVING " + _)).getOrElse("") +
      orderBy.map(_._1.mkString(" ORDER BY ", ",", "")).getOrElse("") +
      ana.limit.map(" LIMIT " + _.toString).getOrElse("") +
      ana.offset.map(" OFFSET " + _.toString).getOrElse("")

    ParametricSql(Seq(countBySubQuery(analysis)(reqRowCount, completeSql)), setParamsOrderBy)
  }

  private def countBySubQuery(analysis: SoQLAnalysis[UserColumnId, SoQLType])(reqRowCount: Boolean, sql: String) =
    if (reqRowCount && analysis.groupBy.isDefined) s"SELECT count(*) FROM ($sql) t_rc" else sql

  /**
   * This cannot handle SoQLLocation because it is mapped to multiple sql columns.
   * SoQLLocation is handled in Sqlizer.
   */
  private val GeoTypes: Set[SoQLType] =
    Set(SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine, SoQLPolygon, SoQLMultiPolygon)

  /**
   * When we pull data out of pg we only want to translate it when we pull it out for performance reasons,
   * in particular if we are doing aggregations on geo types in the SQL query, so we do so against the top
   * level types of the final select list.
   */
  private def toGeoText(sql: String, typ: SoQLType): String = if (GeoTypes.contains(typ)) s"ST_AsBinary($sql)" else sql

  private def select(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                    (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                     setParams: Seq[SetParam],
                     ctx: Context,
                     escape: Escape) = {
    val (sqls, setParamsInSelect) =
      // Chain select handles setParams differently than others.  The current setParams are prepended
      // to existing setParams.  Others are appended.
      // That is why setParams starts with empty in foldLeft.
      analysis.selection.foldLeft(Tuple2(Seq.empty[String], Seq.empty[SetParam])) { (acc, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val ctxSelect = ctx + (RootExpr -> coreExpr) + (SqlizerContext.ColumnName -> columnName.name)
      val (_, selectSetParams) = acc
      val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, selectSetParams, ctxSelect, escape)
      val sqlGeomConverted =
        if (ctx.contains(LeaveGeomAsIs)) { sqls }
        else { sqls.map(toGeoText(_, coreExpr.typ)) }
      (acc._1 ++ sqlGeomConverted, newSetParams)
    }
    (sqls, setParamsInSelect ++ setParams)
  }

  /**
   * Basically, analysis for row count has select, limit and offset removed.
   * TODO: select count(*) // w/o group by which result is always 1.
   * @param a original query analysis
   * @return Analysis for generating row count sql.
   */
  private def rowCountAnalysis(a: SoQLAnalysis[UserColumnId, SoQLType]) = {
    a.copy(selection = a.selection.empty, orderBy = None, limit = None, offset = None)
  }

  private def innermostSoql(a: SoQLAnalysis[_, _], as: Seq[SoQLAnalysis[_, _]] ): Boolean = {
    a.eq(as.head)
  }

  private def outermostSoql(a: SoQLAnalysis[_, _], as: Seq[SoQLAnalysis[_, _]] ): Boolean = {
    a.eq(as.last)
  }
}
