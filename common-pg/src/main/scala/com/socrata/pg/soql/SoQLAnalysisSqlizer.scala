package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.analyzer.JoinHelper
import com.socrata.soql.{SimpleSoQLAnalysis, SoQLAnalysis}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.types._
import com.socrata.soql.typed._

import scala.util.parsing.input.NoPosition

// scalastyle:off import.grouping
object SoQLAnalysisSqlizer extends Sqlizer[AnalysisTarget] {
  import Sqlizer._
  import SqlizerContext._

  def sql(analysis: AnalysisTarget)
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    sql(analysis, reqRowCount = false, rep, typeRep, setParams, ctx, escape)
  }

  /**
   * For rowcount w/o group by, just replace the select with count(*).
   * For rowcount with group by, wrap the original group by sql with a select count(*) from ( {original}) t_rc
   */
  def rowCountSql(analysis: AnalysisTarget)
                 (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  ctx: Context,
                  escape: Escape): ParametricSql = {
    sql(analysis, reqRowCount = true, rep, typeRep, setParams, ctx, escape)
  }

  /**
   * Convert chained analyses to parameteric sql.
   */
  private def sql(analysisTarget: AnalysisTarget, // scalastyle:ignore method.length
                  reqRowCount: Boolean,
                  rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  context: Context,
                  escape: Escape): ParametricSql = {
    val (analyses, tableNames, allColumnReps) = analysisTarget
    val ctx = context + (Analysis -> analyses)
    val firstAna = analyses.head
    val lastAna = analyses.last
    val rowCountForFirstAna = reqRowCount && (firstAna == lastAna)
    val firstCtx = ctx + (OutermostSoql -> outermostSoql(firstAna, analyses)) +
                         (InnermostSoql -> innermostSoql(firstAna, analyses))
    val firstSql = sql(firstAna, None, tableNames, allColumnReps,
                       rowCountForFirstAna, rep, typeRep, setParams, firstCtx, escape)
    var subTableIdx = 0
    val (result, _) = analyses.drop(1).foldLeft((firstSql, analyses.head)) { (acc, ana) =>
      subTableIdx += 1
      val (subParametricSql, prevAna) = acc
      val subTableName = "(%s) AS x%d".format(subParametricSql.sql.head, subTableIdx)
      val tableNamesSubTableNameReplace = tableNames + (TableName.PrimaryTable -> subTableName)
      val subCtx = ctx + (OutermostSoql -> outermostSoql(ana, analyses)) +
                         (InnermostSoql -> innermostSoql(ana, analyses)) -
                         JoinPrimaryTable
      val sqls = sql(ana, Some(prevAna), tableNamesSubTableNameReplace, allColumnReps, reqRowCount &&  ana == lastAna,
          rep, typeRep, subParametricSql.setParams, subCtx, escape)
      (sqls, ana)
    }
    result
  }

  /**
   * Convert one analysis to parameteric sql.
   */
  private def sql(analysis: SoQLAnalysis[UserColumnId, SoQLType], // scalastyle:ignore method.length parameter.number
                  prevAna: Option[SoQLAnalysis[UserColumnId, SoQLType]],
                  tableNames: Map[TableName, String],
                  allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
                  reqRowCount: Boolean,
                  rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[SetParam],
                  context: Context,
                  escape: Escape): ParametricSql = {
    val ana = if (reqRowCount) rowCountAnalysis(analysis) else analysis
    val ctx = context + (Analysis -> analysis) +
                        (TableMap -> tableNames) +
                        (TableAliasMap -> tableNames.map { case (k, v) => (k.alias.getOrElse(k.name), realAlias(k, v)) })

    val joins = JoinHelper.expandJoins(Seq(analysis))

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)

    val joinTableNames = joins.foldLeft(Map.empty[TableName, String]) { (acc, j) =>
      j.alias match {
        case Some(x) =>
          acc + (TableName(x, None) -> x)
        case None =>
          acc
      }
    }

    val joinTableAliases = joins.foldLeft(Map.empty[String, String]) { (acc, j) =>
      j.alias match {
        case Some(x) =>
          acc + (x -> j.tableLike.head.from.get)
        case None =>
          acc
      }
    }

    val tableNamesWithJoins = ctx(TableMap).asInstanceOf[Map[TableName, String]] ++ joinTableNames
    val tableAliasesWithJoins = ctx(TableAliasMap).asInstanceOf[Map[String, String]] ++ joinTableAliases
    val ctxSelectWithJoins = ctxSelect + (TableMap -> tableNamesWithJoins) + (TableAliasMap -> tableAliasesWithJoins)

    val subQueryJoins = joins.filter(x => !SimpleSoQLAnalysis.isSimple(x.tableLike)).map(x => x.tableLike.head.from).toSet
    val repMinusComplexJoinTable = rep.filterKeys(x => !subQueryJoins.contains(x.qualifier))

    val (selectPhrase, setParamsSelect) =
      if (reqRowCount && analysis.groupBy.isEmpty) {
        (Seq("count(*)"), setParams)
      } else {
        select(analysis)(repMinusComplexJoinTable, typeRep, setParams, ctxSelectWithJoins, escape)
      }

    // JOIN
    val (joinPhrase, setParamsJoin) = joins.foldLeft((Seq.empty[String], setParamsSelect)) { (acc, join) =>
      val (sqls, setParams) = acc

      val joinTableName = TableName(join.tableLike.head.from.get, None)
      val joinTableNames = tableNames + (TableName.PrimaryTable -> tableNames(joinTableName))

      val ctxJoin = ctx +
        (SoqlPart -> SoqlJoin) +
        (TableMap -> joinTableNames) +
        (IsSubQuery -> true) +
        (JoinPrimaryTable -> join.tableLike.head.from)
      val joinTableLikeParamSql = Sqlizer.sql((join.tableLike, joinTableNames, allColumnReps))(rep, typeRep, setParams, ctxJoin, escape)
      val joinConditionParamSql = Sqlizer.sql(join.expr)(repMinusComplexJoinTable, typeRep, joinTableLikeParamSql.setParams, ctxSelectWithJoins + (SoqlPart -> SoqlJoin), escape)

      val tableName =
        if (SimpleSoQLAnalysis.isSimple(join.tableLike)) {
          if (join.alias.isEmpty) tableNames(joinTableName)
          else tableNames(joinTableName) + " as " + join.alias.get
        } else {
          val tn = TableName(join.tableLike.head.from.get, join.alias)
          "(" + joinTableLikeParamSql.sql.mkString + ") as " + tn.qualifier
        }

      val joinCondition = joinConditionParamSql.sql.mkString(" ")
      (sqls :+ s" ${join.typ.toString} $tableName ON $joinCondition", joinConditionParamSql.setParams)
    }

    // WHERE
    val where = ana.where.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsJoin, ctxSelectWithJoins + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsJoin)

    // SEARCH
    val search = ana.search.map { search =>
      val searchLit = StringLiteral[SoQLType](search, SoQLText)(NoPosition)
      val ParametricSql(Seq(searchSql), searchSetParams) =
        Sqlizer.sql(searchLit)(repMinusComplexJoinTable, typeRep, setParamsWhere, ctx + (SoqlPart -> SoqlSearch), escape)

      val searchVector = prevAna match {
        case Some(a) => PostgresUniverseCommon.searchVector(a.selection, Some(ctx))
        case None =>
          val primaryTableReps = rep.filter{ case (k, v) => k.qualifier.isEmpty}.values.toSeq
          PostgresUniverseCommon.searchVector(primaryTableReps, Some(ctx))
      }

      val searchNumberLitOpt =
        try {
          val number = BigDecimal.apply(searchLit.value)
          Some(NumberLiteral(number, SoQLNumber.t)(searchLit.position))
        } catch {
          case _: NumberFormatException =>
            None
        }

      val numericCols = searchNumberLitOpt match {
        case None => Seq.empty[String]
        case Some(searchNumberLit) =>
          prevAna match {
            case Some(a) =>
              PostgresUniverseCommon.searchNumericVector(a.selection, Some(ctx))
            case None =>
              val primaryTableReps = rep.filter{ case (k, v) => k.qualifier.isEmpty}.values.toSeq
              PostgresUniverseCommon.searchNumericVector(primaryTableReps, Some(ctx))
          }
      }

      val searchVectorSql = searchVector.map {sv => s"$sv @@ plainto_tsquery('english', $searchSql)" }.toSeq

      val searchPlusNumericParametricSql = numericCols.foldLeft(ParametricSql(searchVectorSql, searchSetParams)) { (acc, x) =>
        val ParametricSql(Seq(prev), setParamsBefore) = acc
        val ParametricSql(Seq(searchSql), setParamsAfter) =
          Sqlizer.sql(searchNumberLitOpt.get)(repMinusComplexJoinTable, typeRep, setParamsBefore, ctx + (SoqlPart -> SoqlSearch), escape)
        ParametricSql(Seq(s"$prev OR ($x = $searchSql)"), setParamsAfter)
      }

      val andOrWhere = if (where.isDefined) " AND" else " WHERE"
      if (searchPlusNumericParametricSql.sql.isEmpty) {
        ParametricSql(Seq(s"$andOrWhere false"), setParamsWhere)
      } else {
        ParametricSql(searchPlusNumericParametricSql.sql.map { x => s"$andOrWhere ($x)" }, searchPlusNumericParametricSql.setParams)
      }
    }

    val setParamsSearch = search.map(_.setParams).getOrElse(setParamsWhere)

    // GROUP BY
    val groupBy = ana.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsSearch)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
        val ParametricSql(sqls, newSetParams) =
          if (Sqlizer.isLiteral(gb)) {
            // SELECT 'literal' as alias GROUP BY 'literal' does not work in SQL
            // But that's how the analysis come back from GROUP BY alias
            // Use group by position
            val groupByColumnPosition = analysis.selection.values.toSeq.indexWhere(_ == gb) + 1
            ParametricSql(Seq(groupByColumnPosition.toString), t2._2)
          } else {
            Sqlizer.sql(gb)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlGroup), escape)
          }
        (t2._1 ++ sqls, newSetParams)

      }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsSearch)

    // HAVING
    val having = ana.having.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsGroupBy, ctxSelectWithJoins + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = ana.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val ParametricSql(sqls, newSetParams) =
          Sqlizer.sql(ob)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
        (t2._1 ++ sqls, newSetParams)
      }}
    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    val tableName = ana.from.map(TableName(_, None)).getOrElse(TableName.PrimaryTable)

    // COMPLETE SQL
    val selectOptionalDistinct = "SELECT " + (if (analysis.distinct) "DISTINCT " else "")
    val completeSql = selectPhrase.mkString(selectOptionalDistinct, ",", "") +
      s" FROM ${tableNames(tableName)}" +
      joinPhrase.mkString(" ") +
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
    Set(SoQLPoint, SoQLMultiPoint, SoQLLine, SoQLMultiLine, SoQLPolygon, SoQLMultiPolygon, SoQLLocation)

  /**
   * When we pull data out of pg we only want to translate it when we pull it out for performance reasons,
   * in particular if we are doing aggregations on geo types in the SQL query, so we do so against the top
   * level types of the final select list.
   */
  private def toGeoText(sql: String, typ: SoQLType, columnName: Option[ColumnName]): String = {
    if (GeoTypes.contains(typ)) { s"ST_AsBinary($sql)" + columnName.map(x => s" AS ${x.name}").getOrElse("") }
    else { sql }
  }

  private def select(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                    (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                     typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                     setParams: Seq[SetParam],
                     ctx: Context,
                     escape: Escape) = {
    val strictInnermostSoql = isStrictInnermostSoql(ctx)
    val strictOutermostSoql = isStrictOutermostSoql(ctx)
    val (sqls, setParamsInSelect) =
      // Chain select handles setParams differently than others.  The current setParams are prepended
      // to existing setParams.  Others are appended.
      // That is why setParams starts with empty in foldLeft.
      analysis.selection.foldLeft(Tuple2(Seq.empty[String], Seq.empty[SetParam])) { (acc, columnNameAndcoreExpr) =>
        val (columnName, coreExpr) = columnNameAndcoreExpr
        val ctxSelect = ctx + (RootExpr -> coreExpr) + (SqlizerContext.ColumnName -> columnName.name)
        val (_, selectSetParams) = acc
        val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, selectSetParams, ctxSelect, escape)
        val sqlGeomConverted =
          if (ctx.contains(LeaveGeomAsIs) || strictInnermostSoql || ctx.contains(IsSubQuery)) { sqls }
          else {
            val cn = if (strictOutermostSoql) Some(columnName) else None
            // compound type with a geometry and something else like "Location" type
            // must place the geometry in the first part.
            if (false == ctx(OutermostSoql) || sqls.isEmpty) { sqls }
            else { sqls.updated(0, toGeoText(sqls.head, coreExpr.typ, cn)) }
          }
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

  private def isStrictOutermostSoql(ctx: Context): Boolean = {
    ctx(OutermostSoql) == true && ctx(InnermostSoql) == false
  }

  private def isStrictInnermostSoql(ctx: Context): Boolean = {
    ctx(InnermostSoql) == true && ctx(OutermostSoql) == false
  }
}
