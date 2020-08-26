package com.socrata.pg.soql

import com.socrata.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.{SoQLAnalysis, SubAnalysis, typed}

import scala.util.parsing.input.NoPosition

object TopSoQLAnalysisSqlizer extends Sqlizer[TopAnalysisTarget] {
  import Sqlizer._

  def sql(analysis: TopAnalysisTarget)
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val (analyses, tableNames, allColumnReps) = analysis
    SoQLAnalysisSqlizer.sql((analyses, None), tableNames, allColumnReps)(rep, typeRep, setParams, ctx, escape)
  }
}

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
    val ((analyses, fromTableName), tableNames, allColumnReps) = analysisTarget
    val ctx = context + (Analysis -> analyses)
    val firstAna = analyses.head
    val lastAna = analyses.last
    val rowCountForFirstAna = reqRowCount && (firstAna == lastAna)
    val firstCtx = ctx + (OutermostSoql -> outermostSoql(firstAna, analyses)) +
                         (InnermostSoql -> innermostSoql(firstAna, analyses))
    val (firstSql, _) = sql(firstAna, None, tableNames, allColumnReps,
                       rowCountForFirstAna, rep, typeRep, setParams, firstCtx, escape, fromTableName)
    val (result, _) = analyses.tail.foldLeft((firstSql, analyses.head)) { (acc, ana) =>
      val (subParametricSql, prevAna) = acc
      val chainedTableAlias = "x1"
      val subTableName = "(%s) AS %s".format(subParametricSql.sql.head, chainedTableAlias)
      val tableNamesSubTableNameReplace = tableNames + (TableName.PrimaryTable -> subTableName)
      val primaryTableAlias =
        if (ana.joins.nonEmpty) Map((PrimaryTableAlias -> chainedTableAlias))
        else Map.empty
      val subCtx = ctx + (OutermostSoql -> outermostSoql(ana, analyses)) +
                         (InnermostSoql -> innermostSoql(ana, analyses)) -
                         JoinPrimaryTable ++ primaryTableAlias
      val (sqls, paramsCountInSelect) = sql(ana, Some(prevAna), tableNamesSubTableNameReplace, allColumnReps, reqRowCount &&  ana == lastAna,
          rep, typeRep, Seq.empty, subCtx, escape)

      // query parameters in the select phrase come before those in the sub-query.
      // query parameters in the other phrases - where, group by, order by, etc come after those in the sub-query.
      val (setParamsBefore, setParamsAfter) = sqls.setParams.splitAt(paramsCountInSelect)
      val setParamsAcc = setParamsBefore ++ subParametricSql.setParams ++ setParamsAfter
      (sqls.copy(setParams = setParamsAcc), ana)
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
                  escape: Escape,
                  fromTableName: Option[String] = None): (ParametricSql, /* params count in select, excluding where, group by... */ Int) = {
    val ana = if (reqRowCount) rowCountAnalysis(analysis) else analysis
    val ctx = context + (Analysis -> analysis) +
                        (TableMap -> tableNames) +
                        (TableAliasMap -> tableNames.map { case (k, v) => (k.qualifier, realAlias(k, v)) })

    val joins = analysis.joins

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)

    val joinTableNames = joins.foldLeft(Map.empty[TableName, String]) { (acc, j) =>
      acc ++ j.from.alias.map(x => TableName(x, None) -> x)
    }

    val joinTableAliases = joins.foldLeft(Map.empty[String, String]) { (acc, j) =>
      acc ++ j.from.alias.map(x => x -> j.from.fromTable.name)
    }

    val tableNamesWithJoins = ctx(TableMap).asInstanceOf[Map[TableName, String]] ++ joinTableNames
    val tableAliasesWithJoins = ctx(TableAliasMap).asInstanceOf[Map[String, String]] ++ joinTableAliases
    val ctxSelectWithJoins = ctxSelect + (TableMap -> tableNamesWithJoins) + (TableAliasMap -> tableAliasesWithJoins)

    val subQueryJoins = joins.filterNot(_.isSimple).map(_.from.fromTable.name).toSet
    val repMinusComplexJoinTable = rep.filterKeys(!_.qualifier.exists(subQueryJoins.contains))

    val (selectPhrase, setParamsSelect) =
      if (reqRowCount && analysis.groupBys.isEmpty && analysis.search.isEmpty) {
        (List("count(*)"), setParams)
      } else {
        select(analysis)(repMinusComplexJoinTable, typeRep, setParams, ctxSelectWithJoins, escape)
      }

    val paramsCountInSelect = setParamsSelect.size - setParams.size

    // JOIN
    val (joinPhrase, setParamsJoin) = joins.foldLeft((Seq.empty[String], Seq.empty[SetParam])) { case ((sqls, setParamAcc), join) =>
      val joinTableName = TableName(join.from.fromTable.name, None)
      val joinTableNames = tableNames + (TableName.PrimaryTable -> tableNames(joinTableName))

      val ctxJoin = ctx +
        (SoqlPart -> SoqlJoin) +
        (TableMap -> joinTableNames) +
        (IsSubQuery -> true) +
        (JoinPrimaryTable -> Some(join.from.fromTable.name))

      val (tableName, joinOnParams) = join.from.subAnalysis.map { case SubAnalysis(analyses, alias) =>
        val joinTableLikeParamSql = Sqlizer.sql(
          ((analyses, Some(join.from.fromTable.name): Option[String]), joinTableNames, allColumnReps))(rep, typeRep, Seq.empty, ctxJoin, escape)
        val tn = "(" + joinTableLikeParamSql.sql.mkString + ") as " + alias
        (tn, setParamAcc ++ joinTableLikeParamSql.setParams)
      }.getOrElse {
        val tableName = tableNames(joinTableName)
        val tn = join.from.alias.map(a => s"$tableName as $a").getOrElse(tableName)
        (tn, setParamAcc)
      }

      val joinConditionParamSql = Sqlizer.sql(join.on)(repMinusComplexJoinTable, typeRep, joinOnParams, ctxSelectWithJoins + (SoqlPart -> SoqlJoin), escape)
      val joinCondition = joinConditionParamSql.sql.mkString(" ")
      (sqls :+ s" ${join.typ.toString} $tableName ON $joinCondition", joinConditionParamSql.setParams)
    }

    val setParamsSelectJoin = setParamsSelect ++ setParamsJoin
    // WHERE
    val where = ana.where.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsSelectJoin, ctxSelectWithJoins + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelectJoin)

    // SEARCH in WHERE
    val ParametricSql(whereSearch, setParamsWhereSearch) = ana.search match {
      case Some(s) if (!ana.isGrouped) =>
        sqlizeSearch(s, ana)(repMinusComplexJoinTable, typeRep, setParamsWhere, ctxSelectWithJoins + (SoqlPart -> SoqlSearch), escape)
      case _ =>
        ParametricSql(Seq.empty[String], setParamsWhere)
    }

    // GROUP BY
    val isGroupByAllConstants = !ana.groupBys.exists(!Sqlizer.isLiteral(_))
    val groupBy = ana.groupBys.foldLeft((List.empty[String], setParamsWhereSearch)) { (t2, gb) =>
      val ParametricSql(sqls, newSetParams) =
        if (Sqlizer.isLiteral(gb)) {
          if (isGroupByAllConstants) {
            // SELECT 'literal' as alias GROUP BY 'literal' does not work in SQL
            // But that's how the analysis come back from GROUP BY alias
            // Use group by position.
            val groupByColumnPosition = analysis.selection.values.toSeq.indexWhere(_ == gb) + 1
            ParametricSql(Seq(groupByColumnPosition.toString), t2._2)
          } else {
            // SoQL does not support group by column position.  Group by literal does not do anything.
            ParametricSql(Seq.empty, t2._2)
          }
        } else {
          Sqlizer.sql(gb)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlGroup), escape)
        }
      (t2._1 ++ sqls, newSetParams)
    }
    val setParamsGroupBy = groupBy._2

    // HAVING
    val having = ana.having.map(Sqlizer.sql(_)(repMinusComplexJoinTable, typeRep, setParamsGroupBy, ctxSelectWithJoins + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // SEARCH in HAVING
    val ParametricSql(havingSearch, setParamsHavingSearch) = ana.search match {
      case Some(s) if (ana.isGrouped) =>
        sqlizeSearch(s, ana)(repMinusComplexJoinTable, typeRep, setParamsHaving, ctxSelectWithJoins + (SoqlPart -> SoqlSearch), escape)
      case _ =>
        ParametricSql(Seq.empty[String], setParamsHaving)
    }

    // ORDER BY
    val orderBy = ana.orderBys.foldLeft((Seq.empty[String], setParamsHavingSearch)) { (t2, ob) =>
      val ParametricSql(sqls, newSetParams) =
        Sqlizer.sql(ob)(repMinusComplexJoinTable, typeRep, t2._2, ctxSelectWithJoins + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
      (t2._1 ++ sqls, newSetParams)
    }
    val setParamsOrderBy = orderBy._2

    val tableName = fromTableName.map(TableName(_, None)).getOrElse(TableName.PrimaryTable)

    // COMPLETE SQL
    val selectOptionalDistinct = "SELECT " + (if (analysis.distinct) "DISTINCT " else "")
    val completeSql = selectPhrase.mkString(selectOptionalDistinct, ",", "") +
      s" FROM ${tableNames(tableName)}" +
      joinPhrase.mkString(" ") +
      where.flatMap(_.sql.headOption.map(" WHERE " +  _)).getOrElse("") +
      //search.flatMap(_.sql.headOption).getOrElse("") +
      whereSearch.mkString(" ") +
      (if (ana.groupBys.nonEmpty) groupBy._1.mkString(" GROUP BY ", ",", "") else "") +
      having.flatMap(_.sql.headOption.map(" HAVING " + _)).getOrElse("") +
      havingSearch.mkString(" ") +
      (if (ana.orderBys.nonEmpty) orderBy._1.mkString(" ORDER BY ", ",", "") else "") +
      ana.limit.map(" LIMIT " + _.toString).getOrElse("") +
      ana.offset.map(" OFFSET " + _.toString).getOrElse("")

    val result = ParametricSql(Seq(countBySubQuery(analysis)(reqRowCount, completeSql)), setParamsOrderBy)
    (result, paramsCountInSelect)
  }

  private def countBySubQuery(analysis: SoQLAnalysis[UserColumnId, SoQLType])(reqRowCount: Boolean, sql: String) =
    if (reqRowCount && (analysis.groupBys.nonEmpty || analysis.search.nonEmpty)) s"SELECT count(*) FROM ($sql) t_rc" else sql


  def sqlizeSearch(searchStr: String, ana: SoQLAnalysis[UserColumnId, SoQLType])
                  (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Context,
                   escape: Escape): ParametricSql = {

    val searchLit = StringLiteral[SoQLType](searchStr, SoQLText)(NoPosition)

    val (searchText, setParamsSearchText) = searchVectorText(ana)(rep, typeRep, setParams, ctx + (SoqlPart -> SoqlSearch), escape)

    val ParametricSql(Seq(searchSql), setParamsSearchLit) =
      Sqlizer.sql(searchLit)(rep, typeRep, setParamsSearchText, ctx + (SoqlPart -> SoqlSearch), escape)

    val searchNumberLitOpt =
      try {
        val number = BigDecimal.apply(searchLit.value)
        Some(NumberLiteral(number, SoQLNumber.t)(searchLit.position))
      } catch {
        case _: NumberFormatException =>
          None
      }

    val (searchVectorSql, searchVectorSetParams) = searchText match {
      case None =>
        (Seq("false"), setParamsSearchText)
      case Some(sv) =>
        (Seq(s"$sv @@ plainto_tsquery('english', $searchSql)"), setParamsSearchLit)
    }

    val (searchNumber, setParamsSearchNumber) = searchNumberLitOpt match {
      case None => (Seq.empty[String], searchVectorSetParams)
      case Some(searchNumberLit) =>
        searchVectorNumber(ana)(rep, typeRep, searchVectorSetParams, ctx + (SoqlPart -> SoqlSearch), escape)
    }

    val searchTextPlusNumberParametricSql = searchNumber.foldLeft(ParametricSql(searchVectorSql, setParamsSearchNumber)) { (acc, x) =>
      val ParametricSql(Seq(prev), setParamsBefore) = acc
      val ParametricSql(Seq(searchSql), setParamsAfter) =
        Sqlizer.sql(searchNumberLitOpt.get)(rep, typeRep, setParamsBefore, ctx + (SoqlPart -> SoqlSearch), escape)
      ParametricSql(Seq(s"$prev OR ($x = $searchSql)"), setParamsAfter)
    }

    val andOrWhereOrHaving = ana.isGrouped match {
      case false => if (ana.where.isDefined) " AND" else " WHERE"
      case true => if (ana.having.isDefined) " AND" else " HAVING"
    }

    if (searchTextPlusNumberParametricSql.sql.isEmpty) {
      ParametricSql(Seq(s"$andOrWhereOrHaving false"), setParams)
    } else {
      ParametricSql(searchTextPlusNumberParametricSql.sql.map { x => s"$andOrWhereOrHaving ($x)" }, searchTextPlusNumberParametricSql.setParams)
    }
  }

  private def searchVectorText(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                              (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                               typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                               setParams: Seq[SetParam],
                               ctx: Context,
                               escape: Escape) = {

    val (sqls, setParamsSearch) =
      analysis.selection.view.filter(x => PostgresUniverseCommon.SearchableTypes.contains(x._2.typ))
                             .foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, columnNameAndcoreExpr) =>
        val (columnName, coreExpr) = columnNameAndcoreExpr
        val (_, setParamsNew) = acc
        val ctxSelect = ctx + (RootExpr -> coreExpr)
        val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, setParamsNew, ctxSelect, escape)
        (acc._1 ++ sqls, newSetParams)
      }
    (PostgresUniverseCommon.toTsVector(sqls.zip(sqls)), setParamsSearch)
  }

  private def searchVectorNumber(analysis: SoQLAnalysis[UserColumnId, SoQLType])
                                (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                 typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                 setParams: Seq[SetParam],
                                 ctx: Context,
                                 escape: Escape) = {

    val (sqls, setParamsSearch) =
    // Chain select handles setParams differently than others.  The current setParams are prepended
    // to existing setParams.  Others are appended.
    // That is why setParams starts with empty in foldLeft.
      analysis.selection.view.filter(x => PostgresUniverseCommon.SearchableNumericTypes.contains(x._2.typ))
                             .foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, columnNameAndcoreExpr) =>
        val (columnName, coreExpr) = columnNameAndcoreExpr
        val ctxSelect = ctx + (RootExpr -> coreExpr)
        val (_, setParamsNew) = acc
        val ParametricSql(sqls, newSetParams) = Sqlizer.sql(coreExpr)(rep, typeRep, setParamsNew, ctxSelect, escape)
        (acc._1 :+ sqls.head, newSetParams) // each sql-string is a column
      }
    (sqls, setParamsSearch)
  }

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
          if (shouldConvertGeomToText(ctx) && !sqls.isEmpty) {
            val cn = if (strictOutermostSoql) Some(columnName) else None
            // compound type with a geometry and something else like "Location" type
            // must place the geometry in the first part.
            sqls.updated(0, toGeoText(sqls.head, coreExpr.typ, cn))
          } else {
            sqls
          }
        (acc._1 ++ sqlGeomConverted, newSetParams)
      }
    (sqls, setParamsInSelect ++ setParams)
  }

  private def shouldConvertGeomToText(ctx: Context): Boolean = {
    !(ctx.contains(LeaveGeomAsIs) || isStrictInnermostSoql(ctx) || ctx.contains(IsSubQuery) || (false == ctx(OutermostSoql)))
  }

  /**
   * Basically, analysis for row count has select (except when there is search), limit and offset removed.
   * TODO: select count(*) // w/o group by which result is always 1.
   * @param a original query analysis
   * @return Analysis for generating row count sql.
   */
  private def rowCountAnalysis(a: SoQLAnalysis[UserColumnId, SoQLType]) = {
    a.copy(selection = if (a.search.isEmpty) a.selection.empty else a.selection,
           orderBys = Nil,
           limit = None,
           offset = None)
  }

  private def innermostSoql(a: SoQLAnalysis[_, _], as: NonEmptySeq[SoQLAnalysis[_, _]] ): Boolean = {
    a.eq(as.head)
  }

  private def outermostSoql(a: SoQLAnalysis[_, _], as: NonEmptySeq[SoQLAnalysis[_, _]] ): Boolean = {
    a.eq(as.last)
  }

  private def isStrictOutermostSoql(ctx: Context): Boolean = {
    ctx(OutermostSoql) == true && ctx(InnermostSoql) == false
  }

  private def isStrictInnermostSoql(ctx: Context): Boolean = {
    ctx(InnermostSoql) == true && ctx(OutermostSoql) == false
  }
}
