package com.socrata.pg.soql

import java.sql.PreparedStatement

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ResourceName, Qualified}
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext.SqlizerContext

import ParametricSqlBuilder.Implicits._

abstract class Sqlizer {
  type ColumnAlias
  type ColRep = SqlColumnReadRep[SoQLType, SoQLValue]
  type Expr = CoreExpr[SoQLColumn, SoQLType]
  type Analysis = SoQLAnalysis[ColumnAlias, SoQLColumn, SoQLType]

  def sqlReadRepFor(alias: ColumnAlias, typ: SoQLType): SqlColumnReadRep[SoQLType, SoQLValue]

  def anonymousSqlWriteRepFor(typ: SoQLType): SqlColumnWriteRep[SoQLType, SoQLValue]
  def anonymousJsonWriteRepFor(typ: SoQLType): JsonColumnWriteRep[SoQLType, SoQLValue]
  def literalCol(lit: TypedLiteral[SoQLType]): SoQLColumn

  def primaryTableSqlName(ref: TableRef.PrimaryCandidate): String
  def tableRefAlias(ref: TableRef): String

  def apply(analyses: NonEmptySeq[Analysis]): ParametricSql =
    convertOutput(analyses.head.selection, convertChain(analyses, TableRef.Primary), topLevel = true)

  implicit class QueryHelper(private val sc: StringContext) extends AnyVal {
    def sql(args: ParametricSqlBuilder*): ParametricSqlBuilder.Append = new ParametricSqlBuilder.Append {
      def appendTo(builder: ParametricSqlBuilder) {
        val strings = sc.parts.iterator
        val exprs = args.iterator
        appendTo.appendRawSql(strings.next())
        while(strings.hasNext) {
          appendTo.
            appendRawSql("(").
            appendParametricSql(exprs.next()).
            appendRawSql(")").
            appendRawSql(strings.next())
        }
      }
    }
  }

  private def convertChain(analyses: NonEmptySeq[Analysis], fromRef: TableRef.PrimaryCandidate, topLevel: Boolean): ParametricSql = {
    val initialFrom =
      convertStep(analyses.head, new ParametricSqlBuilder().
                    appendRawSql(primaryTableSqlName(fromRef)).
                    appendRawSql("AS").
                    appendRawSql(tableRefAlias(fromRef)),
                  topLevel = topLevel && analyses.tail.isEmpty)
    val lastAnalysis = analyses.tail.length - 1
    analyses.tail.iterator.zipWithIndex.foldLeft(initialFrom) { (from, analysisAndIndex) =>
      val (analysis, index) = analysisAndIndex
      convertStep(analysis, from, topLevel = topLevel && index == lastAnalysis)
    }
  }

  private def convertStep(analysis: Analysis, from: ParametricSql, topLevel: Boolean): ParametricSqlBuilder = {
    // TODO: handle search
    val SoQLAnalyis(_, _, distinct, selection, joins, where, groupBys, having, orderBys, limit, offset, search) = analysis

    val result = new ParametricSqlBuilder

    result.appendRawSql("SELECT")
    if(distinct) result.appendRawSql("DISTINCT")
    result.appendParametricSql(convertSelection(selection, topLevel)).
      appendRawSql("FROM").
      appendParametricSql(from).
      appendParametricSql(convertJoins(joins)).
      appendParametricSql(convertWhere(where)).
      appendParametricSql(convertGroupBys(groupBys)).
      appendParametricSql(convertHaving(having)).
      appendParametricSql(convertOrderBys(orderBys)).
      appendParametricSql(convertLimit(limit)).
      appendParametricSql(convertOffset(offset))
  }

  private def convertSelection(selection: TraversableOnce[(ColumnAlias, Expr)], topLevel: Boolean): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder

    var didOne = false
    for((baseAlias, expr) <- selection) {
      val rep = sqlReadRepFor(baseAlias, expr.typ)
      val fragments = convertExpr(expr).toArray

      assert(fragments.length == rep.physColumns.length)

      if(topLevel) {
        val transformed = rep.selectListTransforms
        (fragments, transformed).foreach { fragmentXform =>
          val (fragment, (prefix, suffix)) = fragmentXform
          if(didOne) result.appendRawSql(",")
          else didOne = true
          result.appendRawSql(prefix).
            appendParametricSql(exprFragment).
            appendRawSql(suffix)
        }
      } else {
        (fragments, rep.physColumns).zipped.foreach { (exprFragment, alias) =>
          if(didOne) result.appendRawSql(",")
          else didOne = true
          result.appendParametricSql(exprFragment).
            appendSql("AS").
            appendSql(alias)
        }
      }
    }

    result
  }

  private def convertJoins(joins: Seq[Join]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    for(join <- joins) result ++= convertJoin(join)
    result
  }

  private def convertJoin(join: Join): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    result.appendRawSql(join.from.toString). // ick
      appendParametricSql(convertJoinFrom(join.from)).
      appendRawSql("ON").
      appendParametricSql(convertBooleanExpr(join.on))
    result
  }

  def convertJoinFrom(ja: JoinAnalysis[SoQLColumn, SoQLType]): ParametricSqlBuilder = {
    ja match {
      case jta: JoinTableAnalysis[SoQLColumn, SoQLType] =>
        convertJoinFromTable(jta)
      case jsa: JoniSelectAnalysis[SoQLColumn, SoQLType] =>
        convertJoinFromSubselect(jsa)
    }
  }

  def convertJoinFromSubselect(jsa: JoinSelectAnalysis[SoQLColumn, SoQLType]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    result.appendRawSql("(").
      appendParametricSql(convertChain(jsa.analyses, jsa.fromTable)).
      appendRawSql(") AS ").
      appendRawSql(tableRefAlias(jsa.outputTable))
    result
  }

  def convertJoinFromTable(jta: JoinTableAnalysis[SoQLColumn, SoQLType]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    result.appendRawSql(primaryTableSqlName(jta.fromTable)).
      appendRawSql("AS").
      appendRawSql(tableRefAlias(jta.outputTable))
    result
  }

  private def convertWhere(where: Option[Expr]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    for(expr <- where) {
      result.appendRawSql("WHERE").appendParametricSql(convertBooleanExpr(expr))
    }
    result
  }

  private def convertGroupBys(groupBy: Seq[Expr]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    if(groupBys.nonEmpty) {
      result.appendRawSql("GROUP BY")
      var didOne = false
      for {
        groupBy <- groupBys
        exprSql <- convertExpr(groupBy)
      } {
        if(didOne) result.appendRawSql(",")
        else didOne = true
        result.appendParametricSql(exprSql)
      }
    }
    result
  }

  private def convertHaving(having: Option[Expr]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    for(expr <- having) {
      result.appendRawSql("HAVING").appendParametricSql(convertBooleanExpr(expr))
    }
    result
  }

  private def convertOrderBys(orderBys: Option[Expr]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    if(orderBys.nonEmpty) {
      result.appendRawSql("ORDER BY")
      var didOne = false
      for {
        ob <- orderBys
        expr <- convertExpr(ob.expr)
      } {
        if(didOne) result.append(",")
        else didOne = true
        result.appendParametricSql(expr)
        if(!ob.ascending) appendRawSql("DESC")
        if(ob.nullLast) appendRawSql("NULLS LAST")
      }
    }
    result
  }

  private def convertLimit(limit: Option[BigInt]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    for(l <- limit) {
      result.appendRawSql("LIMIT").
        appendRawSql(l.toString)
    }
    result
  }

  private def convertOffset(offset: Option[BigInt]): ParametricSqlBuilder = {
    val result = new ParametricSqlBuilder
    for(o <- offset) {
      result.appendRawSql("OFFSET").
        appendRawSql(o.toString)
    }
    result
  }

  private def convertBooleanExpr(expr: Expr): ParametricSqlBuilder = {
    assert(expr.typ == SoQLBoolean)
    val fragments = convertExpr(expr)
    assert(fragments.length == 1)
    fragments.head
  }

  private def convertExpr(expr: Expr): Seq[ParametricSqlBuilder] =
    convertExprToColumn(expr).sqlFragments.toSeq

  private def convertExprToColumn(expr: Expr): SoQLColumn =
    expr match {
      case cr@ColumnRef(_, _) => convertColumnRef(cr)
      case fc@FunctionCall(_, _) => convertFunctionCall(fc)
      case lit: TypedLiteral[SoQLType] => literalCol(lit)
    }

  private def convertColumnRef(cr: ColumnRef[SoQLColumn, SoQLValue]): SoQLColumn = {
    assert(cr.typ == cr.column.typ)
    cr.column
  }

  private def convertFunctionCall(fc: FunctionCall[SoQLColumn, SoQLValue]): Seq[ParametricSqlBuilder] = {
    checkedRenderer(fc.function, functions(fc.function.identity)).render(fc.params)
  }

  private type FunctionRenderer = Seq[Expr] => SoQLColumn

  def checkedRenderer(mf: MonomorphicFunction[SoQLType], r: FunctionRenderer): FunctionRenderer = { params =>
    require(mf.matches(params.map(_.typ)))
    val result = r(params)
    assert(mf.result == result.typ)
    result
  }

  private val functions = Map[String, FunctionRenderer](
    SoQLFunctions.IsNull.function.identity -> convertIsNull,
    SoQLFunctions.IsNotNull.function.identity -> convertIsNotNull,
    SoQLFunctions.Not.function.identity -> convertNot,
    SoQLFunctions.In.function.identity -> convertIn,
    SoQLFunctions.NotIn.function.identity -> convertNotIn,
    SoQLFunctions.Eq.function.identity -> convertEq,
    SoQLFunctions.EqEq.function.identity -> convertEq,
    SoQLFunctions.Neq.function.identity -> convertNeq,
    SoQLFunctions.BangEq.function.identity -> convertNeq,
    SoQLFunctions.And.function.identity -> simple2(SoQLBoolean) { (a, b) => sql("$a AND $b") },
    SoQLFunctions.Or.function.identity -> simple2(SoQLBoolean) { (a, b) => sql("$a OR $b") },
    SoQLFunctions.NotBetween.function.identity -> convertNotBetween,
    SoQLFunctions.Between.function.identity -> convertBetween,
    SoQLFunctions.Lt.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a < $b" },
    SoQLFunctions.Lte.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a <= $b" },
    SoQLFunctions.Gt.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a > $b" },
    SoQLFunctions.Gte.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a >= $b" },
    // TextToRowIdentifier, TextToRowVersion: figure out how to do this over in typechecking
    SoQLFunctions.Like.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a LIKE $b" },
    SoQLFunctions.NotLike.function.identity -> simple2(SoQLBoolean) { (a, b) => sql"$a NOT LIKE $b" },
    SoQLFunctions.StartsWith.function.identity -> convertsStartsWith, // not just "$a LIKE ($b || '%')" so we can optimize the text-literal case
    SoQLFunctions.Contains.function.identity -> convertsContains, // ditto
    SoQLFunctions.Concat.function.identity -> simple2(SoQLText) { (a, b) => sql"$a || $b" },
    SoQLFunctions.Lower.function.identity -> simple1(PreserveType) { a => sql"lower($a)" },
    SoQLFunctions.Upper.function.identity -> simple1(PreserveType) { a => sql"upper($a)" },
    SoQLFunctions.UnaryPlus.function.identity -> simple1(PreserveType) { a => a },
    SoQLFunctions.UnaryMinus.function.identity -> simple1(PreserveType) { a => sql"-$a" },
    SoQLFunctions.SignedMagnitude10.function.identity -> simple1(PreserveType) { a => sql"sign($a) * length(floor(abs($a)) :: text)" },
    SoQLFunctions.SignedMagnitudeLinear.function.identity -> simple2(PreserveType(0)) { (a, b) => sql"case when $b = 1 then floor($b) else sign($b) * floor(abs($b)/%a + 1) end" },
    SoQLFunctions.BinaryPlus.function.identity -> simple2(PreserveType(0)) { (a, b) => sql"$a + $b" },
    SoQLFunctions.BinaryMinus.function.identity -> simple2(PreserveType(0)) { (a, b) => sql"$a - $b" },
    SoQLFunctions.TimesNumNum.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a * $b" },
    SoQLFunctions.TimesNumMoney.function.identity -> simple2(SoQLMoney) { (a, b) => sql"$a * $b" },
    SoQLFunctions.TimesMoneyNum.function.identity -> simple2(SoQLMoney) { (a, b) => sql"$a * $b" },
    SoQLFunctions.DivNumNum.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a / $b" },
    SoQLFunctions.DivDoubleDouble.function.identity -> simple2(SoQLDouble) { (a, b) => sql"$a / $b" },
    SoQLFunctions.DivMoneyNum.function.identity -> simple2(SoQLMoney) { (a, b) => sql"$a / $b" },
    SoQLFunctions.DivMoneyMoney.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a / $b" },
    SoQLFunctions.ExpNumNum.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a / $b" },
    SoQLFunctions.ExpDoubleDouble.function.identity -> simple2(SoQLDouble) { (a, b) => sql"$a / $b" },
    SoQLFunctions.ModNumNum.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a % $b" },
    SoQLFunctions.ModDoubleDouble.function.identity -> simple2(SoQLDouble) { (a, b) => sql"$a % $b" },
    SoQLFunctions.ModMoneyNum.function.identity -> simple2(SoQLMoney) { (a, b) => sql"$a % $b" },
    SoQLFunctions.ModMoneyMoney.function.identity -> simple2(SoQLNumber) { (a, b) => sql"$a % $b" },
    SoQLFunctions.Absolute.function.identity -> simple1(PreserveType) { a => sql"abs($a)" },
    SoQLFunctions.Ceil.function.identity -> simple1(PreserveType) { a => sql"ceil($a)" },
    SoQLFunctions.Floor.function.identity -> simple1(PreserveType) { a => sql"floor($a)" },

    SoQLFunctions.FloatingTimeStampTruncYmd.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('day', $a)" },
    SoQLFunctions.FloatingTimeStampTruncYm.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('month', $a)" },
    SoQLFunctions.FloatingTimeStampTruncY.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('year', $a)" },

    SoQLFunctions.FloatingTimeStampExtractY.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(year from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractM.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(month from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractD.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(day from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractHh.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(hour from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractMm.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(minute from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractSs.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(second from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractDow.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(dow from $a)::numeric" },
    SoQLFunctions.FloatingTimeStampExtractWoy.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"extract(week from $a)::numeric" },

    SoQLFunctions.FixedTimeStampZTruncYmd.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('day', $a)" },
    SoQLFunctions.FixedTimeStampZTruncYm.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('month', $a)" },
    SoQLFunctions.FixedTimeStampZTruncY.function.identity -> simple1(SoQLFloatingTimestamp) { a => sql"date_trunc('year', $a)" },

    SoQLFunctions.FixedTimeStampTruncYmdAtTimeZone.identity -> simple2(SoQLFloatingTimestamp) { (a, b) => sql"date_trunc('day', $a at time zone $b)" },
    SoQLFunctions.FixedTimeStampTruncYmAtTimeZone.identity -> simple2(SoQLFloatingTimestamp) { (a, b) => sql"date_trunc('month', $a at time zone $b)" },
    SoQLFunctions.FixedTimeStampTruncYAtTimeZone.identity -> simple2(SoQLFloatingTimestamp) { (a, b) => sql"date_trunc('year', $a at time zone $b)" },

    SoQLFunctions.TimeStampDiffD.identity -> simple2(SoQLNumber) { (a, b) => sql"trunc((extract(epoch from $a) - extract(epoch from $b))::numeric / 86400" },

    SoQLFunctions.ToFloatingtimestamp.identity -> simple2(SoQLFloatingTimestamp) { (a, b) => sql"$a at time zone $b" },

    SoQLFunctions.NumberToText.identity -> simple1(SoQLText) { a => sql"$a :: varchar" },
    SoQLFunctions.NumberToMoney.identity -> simple1(SoQLMoney) { a => a },
    SoQLFunctions.NumberToDouble.identity -> simple1(SoQLDouble) { a => sql"$a :: float" },
    SoQLFunctions.TextToNumber.identity -> simple1(SoQLNumber) { a => sql"$a :: numeric" },

    // These do a thing I don't understand in the original, and I
    // suspect them to be wrong or at least inconsistent if given a non-literal
    // SoQLFunctions.TextToFixedTimestamp.identity -> ???,
    // SoQLFunctions.TextToFloatingTimestamp.identity -> ???,
    SoQLFunctions.TextToMoney.identity -> simple1(SoQLMoney) { a => sql"$a :: numeric" },
    SoQLFunctions.TextToBlob.identity -> simple1(SoQLBlob) { a => a },
    SoQLFunctions.TextToPhoto.identity -> simple1(SoQLPhoto) { a => a },

    SoQLFunctions.TextToBool.identity -> simple1(SoQLBoolean) { a => sql"$a :: boolean" },
    SoQLFunctions.BoolToText.identity -> simple1(SoQLText) { a => sql"$a :: varchar" },

    SoQLFunctions.Case.identity -> convertCase,
    SoQLFunctions.Coalesce.identity -> convertCoalesce,

    SoQLFunctions.Avg.identity -> simple1(PreserveType) { a => sql"avg($a)" },
    SoQLFunctions.Min.identity -> simple1(PreserveType) { a => sql"min($a)" },
    SoQLFunctions.Max.identity -> simple1(PreserveType) { a => sql"max($a)" },
    SoQLFunctions.Sum.identity -> simple1(PreserveType) { a => sql"sum($a)" },
    SoQLFunctions.StddevPop.identity -> simple1(PreserveType) { a => sql"stddev_pop($a)" },
    SoQLFunctions.StddevSamp.identity -> simple1(PreserveType) { a => sql"stddev_samp($a)" },
    SoQLFunctions.Median.identity -> convertMedian,
    SoQLFunctions.MedianDisc.identity -> simple1(PreserveType) { a => sql"percentile_disc(.50) within group (order by $a)" },
  )

  private def reduceInfix(resultType: SoQLType, op: String, exprs: IntermediateSoQLColumn): SoQLColumn =
    new SoQLColumn {
      def typ = resultType
      val builder = exprs.map(_.surrounded("(", ")")).mkSql(op)
      def sqlFragments = Seq(builder)
    }

  def reduceAnd(exprs: IntermediateSoQLColumn): SoQLColumn =
    reduceInfix(SoQLBoolean, "AND", exprs)

  def reduceOr(exprs: IntermediateSoQLColumn): SoQLColumn =
    reduceInfix(SoQLBoolean, "OR", exprs)

  private def extract1(params: Seq[Expr]): Expr =
    params match {
      case Seq(value) => value
      case _ => throw new ArityMismatch(1, params.length)
    }

  private def extract1C(params: Seq[Expr]): SoQLColumn = convertExprToColumn(extract1(params))

  private def extract2(params: Seq[Expr]): (Expr, Expr) =
    params match {
      case Seq(value1, value2) => (value1, value2)
      case _ => throw new ArityMismatch(2, params.length)
    }

  private def extract2C(params: Seq[Expr]): (SoQLColumn, SoQLColumn) = {
    val (a, b) = extract2(params)
    (convertExprToColumn(a), convertExprToColumn(b))
  }

  private def extract3(params: Seq[Expr]): (Expr, Expr, Expr) =
    params match {
      case Seq(value1, value2, value3) => (value1, value2, value3)
      case _ => throw new ArityMismatch(3, params.length)
    }

  private def extract3C(params: Seq[Expr]): (SoQLColumn, SoQLColumn, SoQLColumn) = {
    val (a, b, c) = extract3(params)
    (convertExprToColumn(a), convertExprToColumn(b), convertExprToColumn(c))
  }

  private def convertIsNull(params: Seq[Expr]) = {
    reduceAnd(extract1C(params).mapFragments { v => sql"$v IS NULL" })
  }

  private def convertIsNotNull(params: Seq[Expr]) = {
    reduceOr(extract1C(params).mapFragments { v => sql"$v IS NOT NULL" })
  }

  private def convertNot(params: Seq[Expr]) = {
    val col = extract1C(params)
    new SoQLColumn {
      val typ = col.typ
      def sqlFragments = col.sqlFragments.map { v => sql"NOT $v" }
    }
  }

  private def convertIn(params: Seq[Expr]) = {
    require(params.length >= 2)
    require(params.all(_.typ == params.head.typ))
    val converted = params.map(convertExprToColumn)

    val scrutinee = converted.head
    val others = converted.tail

    require(others.all(_.typ == scrutinee.typ))

    if(scrutinee.length == 1) { // simple type; actually use a SQL IN
      sql"""$scrutinee IN ${converted.mkSql(",")}"""
    } else {
      // compound type; convert to disjoined equality compares
      reduceOr(others.map { other => convertEqSql(scrutinee, other) })
    }
  }

  private def convertNotIn(params: Seq[Expr]) = {
    require(params.length >= 2)
    require(params.all(_.typ == params.head.typ))
    val converted = params.map(convertExprToColumn)

    val scrutinee = converted.head
    val others = converted.tail

    assert(others.all(_.typ == scrutinee.typ))

    if(scrutinee.length == 1) { // simple type; actually use a SQL IN
      sql"""$scrutinee NOT IN ${converted.mkSql(",")}"""
    } else {
      // compound type; convert to disjoined equality compares
      reduceAnd(others.map { other => convertNeqSql(scrutinee, other) })
    }
  }

  private val jsonbFields = Map(DocumentToFilename.function.identity -> "filename",
                                DocumentToFileId.function.identity -> "file_id",
                                DocumentToContentType.function.identity -> "content_type")

  private def convertEq(params: Seq[Expr]) = {
    val (a, b) = extract2(params)
    // ugggh... this special-casing wouldn't be necessary if Document
    // had a multi-column representation instead of being a jsonb blob
    // (actually, _is_ it necessary?  It's copied over from the old
    // version but is it actually a win or would the destructuring
    // version `aSql ->> 'field' == bSql` work just as well?)
    a match {
      case FunctionCall(mf, Seq(field)) if jsonbFields.contains(mf.function.identity) =>
        b match {
          case StringLiteral(v, SoQLText) =>
            // fortunately, at least we know we're taking strings in
            // and putting a boolean out, so we we know everything has
            // width 1.
            val fieldName = jsonbFields(mf.function.identity)
            val Seq(aSql) = convertExpr(a)
            val bJson = json"""{$fieldName: $v}"""
            return new SoQLColumn {
              def typ = SoQLBoolean
              def sqlFragments = Seq(sql"""$aSql @>""".appendJsonbParameter(bJson))
            }
          case _ =>
            // fall through
        }
      case _ =>
        // fall through
    }

    convertEqSql(convertExprToColumn(a), convertExprToColumn(b))
  }

  private def convertEqSql(a: SoQLColumn, b: SoQLColumn) = {
    require(a.typ == b.typ)
    reduceAnd(new IntermediateSoQLColumn {
                def sqlFragments = (a.sqlFragments, b.sqlFragments).zipped.map { (a1, b1) => sql"$a1 = $b1" }
              })
  }

  private def convertNeq(params: Seq[Expr]) = {
    val (a, b) = extract2C(params)
    convertNeqSql(a, b)
  }

  private def convertNeqSql(a: Seq[ParametricSqlBuilder], b: Seq[ParametricSqlBuilder]) = {
    require(a.length == b.length)
    reduceOr(new IntermediateSoQLColumn {
                def sqlFragments = (a.sqlFragments, b.sqlFragments).zipped.map { (a1, b1) => sql"$a1 <> $b1" }
              })
  }

  private def convertBetween(params: Seq[Expr]) = {
    val (scrutinee, lowerBound, upperBound) = extract3C(params)

    require(scrutinee.typ == lowerBound.typ)
    require(scrutinee.typ == upperBound.typ)
    require(SoQLTypeClasses.Ordered(scrutinee.typ))

    reduceAnd(new IntermediateSoQLColumn {
                def sqlFragments =
                  (scrutineeSql.sqlFragments, lowerBound.sqlFragments, upperBound.sqlFragments).zipped.map { (s, l, u) =>
                    sql"""$s BETWEEN $l AND $u"""
                  }
              })
  }

  private def convertNotBetween(params: Seq[Expr]) = {
    val (scrutinee, lowerBound, upperBound) = extract3C(params)

    require(scrutinee.typ == lowerBound.typ)
    require(scrutinee.typ == upperBound.typ)
    require(SoQLTypeClasses.Ordered(scrutinee.typ))

    reduceAnd(new IntermediateSoQLColumn {
                def sqlFragments =
                  (scrutineeSql.sqlFragments, lowerBound.sqlFragments, upperBound.sqlFragments).zipped.map { (s, l, u) =>
                    sql"""$s NOT BETWEEN $l AND $u"""
                  }
              })
  }

  private def convertCoalesce(params: Seq[Expr]) = {
    new SoQLColumn {
      require(params.length >= 1)
      val typ = params.head.typ
      require(params.tail.forall(_.typ == typ))
      val converted = params.map(convertExprToColumn).map(_.sqlFragments)
      def sqlFragments = sql"""coalesce${converted.mkSql(",")}"""
    }
  }

  def simple1(t: SoQLType)(f: ParametricSqlBuilder => ParametricSqlBuilder): (Seq[Expr] => SoQLColumn) = { params =>
    val a = extract1C(params)
    val aFrags = a.sqlFragments
    require(aFrags.length == 1)
    require(a.typ == t)
    new SoQLColumn {
      def sqlFragments = Seq(f(aFrags.head))
      def typ = t
    }
  }

  case class PreserveType(index: Int)
  object PreserveType

  def simple1(pt: PreserveType.type)(f: ParametricSqlBuilder => ParametricSqlBuilder): (Seq[Expr] => SoQLColumn) = { params =>
    val a = extract1C(params)
    val aFrags = a.sqlFragments
    require(aFrags.length == 1)
    new SoQLColumn {
      def sqlFragments = Seq(f(aFrags.head))
      def typ = a.typ
    }
  }

  def simple2(t: SoQLType)(f: (ParametricSqlBuilder, ParametricSqlBuilder) => ParametricSqlBuilder): (Seq[Expr] => SoQLColumn) = { params =>
    val (a, b) = extract2C(params)
    val aFrags = a.sqlFragments
    val bFrags = b.sqlFragments
    require(aFrags.length == 1)
    require(bFrags.length == 1)
    new SoQLColumn {
      def sqlFragments = Seq(f(aFrags.head, bFrags.head))
      def typ = t
    }
  }

  def simple2(t: PreserveType)(f: (ParametricSqlBuilder, ParametricSqlBuilder) => ParametricSqlBuilder): (Seq[Expr] => SoQLColumn) = { params =>
    val (a, b) = extract2C(params)
    val aFrags = a.sqlFragments
    val bFrags = b.sqlFragments
    require(aFrags.length == 1)
    require(bFrags.length == 1)
    new SoQLColumn {
      def sqlFragments = Seq(f(aFrags.head, bFrags.head))
      val typ = params(t.index).typ
    }
  }

}
