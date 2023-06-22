package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions.{Function, MonomorphicFunction, SoQLTypeInfo}

class SoQLFunctionSqlizer[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })] extends FuncallSqlizer[MT] {
  import SoQLTypeInfo.hasType

  def wrap(e: Expr, exprSql: ExprSql, wrapper: String, additionalWrapperArgs: Doc*) =
    ExprSql((exprSql.compressed.sql +: additionalWrapperArgs).funcall(Doc(wrapper)), e)

  def numericize(sqlizer: OrdinaryFunctionSqlizer) = ofs { (f, args, ctx) =>
    val e = sqlizer(f, args, ctx)
    assert(e.typ == SoQLNumber)
    ExprSql(e.compressed.sql +#+ d":: numeric", f)
  }

  def numericize(sqlizer: AggregateFunctionSqlizer) = afs { (f, args, filter, ctx) =>
    val e = sqlizer(f, args, filter, ctx)
    assert(e.typ == SoQLNumber)
    ExprSql(e.compressed.sql +#+ d":: numeric", f)
  }

  def numericize(sqlizer: WindowedFunctionSqlizer) = wfs { (f, args, filter, partitionBy, orderBy, ctx) =>
    val e = sqlizer(f, args, filter, partitionBy, orderBy, ctx)
    assert(e.typ == SoQLNumber)
    ExprSql(e.compressed.sql +#+ d":: numeric", f)
  }

  def sqlizeNormalOrdinaryWithWrapper(name: String, wrapper: String) = ofs { (f, args, ctx) =>
    val exprSql = sqlizeNormalOrdinaryFuncall(name)(f, args, ctx)
    wrap(f, exprSql, wrapper)
  }

  def sqlizeNormalAggregateWithWrapper(name: String, wrapper: String) = afs { (f, args, filter, ctx) =>
    val exprSql = sqlizeNormalAggregateFuncall(name)(f, args, filter, ctx)
    wrap(f, exprSql, wrapper)
  }

  def sqlizeNormalWindowedWithWrapper(name: String, wrapper: String) = wfs { (f, args, filter, partitionBy, orderBy, ctx) =>
    val exprSql = sqlizeNormalWindowedFuncall(name)(f, args, filter, partitionBy, orderBy, ctx)
    wrap(f, exprSql, wrapper)
  }

  def sqlizeMultiBuffered(name: String) = ofs { (f, args, ctx) =>
    val exprSql = sqlizeNormalOrdinaryFuncall(name)(f, args, ctx)
    wrap(f, wrap(f, exprSql, "st_buffer", d"0.0"), "st_multi")
  }

  val defaultSRIDLiteral = d"4326"

  def sqlizeGeomCast(sqlFunctionName: String) = ofs { (f, args, ctx) =>
    // Just like a normal ordinary function call, but with an
    // additional synthetic parameter for SRID
    //
    // TODO: these casts return NULL on
    // valid-but-not-actually-this-type WKT.  Might be better to raise
    // an error then?  Would have to create a temporary for the
    // argument if it's nontrivial.  Won't happen if the cast is being
    // introduced implicitly.
    assert(f.function.minArity == 1 && !f.function.isVariadic)
    assert(f.function.allParameters == args.map(_.typ))

    val sql = (args.map(_.compressed.sql) :+ defaultSRIDLiteral).funcall(Doc(sqlFunctionName))

    ExprSql(sql.group, f)
  }

  def sqlizeCase = ofs { (f, args, ctx) =>
    assert(f.function.function eq Case)
    assert(args.length % 2 == 0)
    assert(args.length >= 2)

    val lastCase = args.takeRight(2)

    def sqlizeCondition(conditionConsequent: Seq[ExprSql]) = {
      val Seq(condition, consequent) = conditionConsequent
      ((d"WHEN" +#+ condition.compressed.sql).nest(2).group ++ Doc.lineSep ++ (d"THEN" +#+ consequent.compressed.sql).nest(2).group).nest(2).group
    }

    val clauses =
      lastCase.head.expr match {
        case LiteralValue(SoQLBoolean(true)) =>
          val initialCases = args.dropRight(2)
          val otherwise = lastCase(1)
          initialCases.grouped(2).map(sqlizeCondition).toSeq :+ (d"ELSE" +#+ otherwise.compressed.sql).nest(2).group
        case _ =>
          args.grouped(2).map(sqlizeCondition).toSeq
      }

    val doc = (d"CASE" ++ Doc.lineSep ++ clauses.vsep).nest(2) ++ Doc.lineSep ++ d"END"

    ExprSql(doc.group, f)
  }

  def sqlizeContains = ofs { (f, args, ctx) =>
    assert(args.length == 2)
    val sql = Seq(args(1).compressed.sql.parenthesized +#+ d"in" +#+ args(0).compressed.sql.parenthesized).funcall(d"position") +#+ d"<> 0"
    ExprSql(sql.group, f)
  }

  def sqlizeIif = ofs { (f, args, ctx) =>
    assert(f.function.function eq Iif)
    assert(args.length == 3)

    val clauses = Seq(
      ((d"WHEN" +#+ args(0).compressed.sql).nest(2).group ++ Doc.lineSep ++ (d"THEN" +#+ args(1).compressed.sql).nest(2).group).nest(2).group,
      (d"ELSE" +#+ args(2).compressed.sql).nest(2).group
    )

    val doc = (d"CASE" ++ Doc.lineSep ++ clauses.vsep).nest(2) ++ Doc.lineSep ++ d"END"

    ExprSql(doc.group, f)
  }

  def sqlizeGetContext = ofs { (f, args, ctx) =>
    // ok, args have already been sqlized if we want to use them, but
    // we only want to use them if it wasn't a literal value.
    assert(f.function.function eq GetContext)
    assert(args.length == 1)
    assert(f.args.length == 1)
    assert(f.typ == SoQLText)

    def nullLiteral =
      ctx.repFor(SoQLText).nullLiteral(NullLiteral[MT](SoQLText)(f.position.asAtomic))(ctx.gensymProvider)
        .withExpr(f)

    f.args(0) match {
      case lit@LiteralValue(SoQLText(key)) =>
        ctx.systemContext.get(key) match {
          case Some(value) =>
            ctx.repFor(SoQLText).literal(LiteralValue[MT](SoQLText(value))(f.position.asAtomic))(ctx.gensymProvider)
              .withExpr(f)
          case None =>
            nullLiteral
        }
      case e@NullLiteral(typ) =>
        nullLiteral
      case _ =>
        ctx.nonliteralSystemContextLookupFound = true
        val hashedArg = Seq(args(0).compressed.sql).funcall(d"md5").group
        val prefixedArg = d"'socrata_system.a' ||" +#+ hashedArg
        val lookup = Seq(prefixedArg.group, d"true").funcall(d"current_setting")
        ExprSql(lookup, f)
    }
  }

  def sqlizePad(name: String) = {
    val underlyingSqlizer = Doc(name)
    ofs { (f, args, ctx) =>
      assert(args.length == 3)
      ExprSql(
        Seq(
          args(0).compressed.sql,
          args(1).compressed.sql.parenthesized +#+ d":: int",
          args(2).compressed.sql
        ).funcall(underlyingSqlizer),
        f
      )
    }
  }

  def sqlizeExtractTimestampField(field: String) = {
    val fieldDoc = Doc(field)
    ofs { (f, args, ctx) =>
      assert(args.length == 1)
      val sql =
        Seq(
          Seq(fieldDoc +#+ d"from" +#+ args(0).compressed.sql.parenthesized).funcall(d"extract") +#+ d":: numeric",
          d"3"
        ).funcall(d"round")
      ExprSql(sql, f)
    }
  }

  def sqlizeTimestampDiffD = ofs { (f, args, ctx) =>
    assert(args.length == 2)

    val delta =
      (
        Seq(d"epoch from" +#+ args(0).compressed.sql.parenthesized).funcall(d"extract") +#+
          d"-" +#+ Seq(d"epoch from" +#+ args(1).compressed.sql.parenthesized).funcall(d"extract")
      ).group

    val sql =
      Seq(
        (delta +#+ d":: numeric").parenthesized +#+ d"/" +#+ d"86400"
      ).funcall(d"trunc")
    ExprSql(sql, f)
  }

  def pointSqlFromLocationSql(sql: ExprSql): Doc = {
    assert(sql.typ == SoQLLocation)
    sql match {
      case expanded: ExprSql.Expanded[MT] =>
        expanded.sqls.head
      case compressed: ExprSql.Compressed[MT] =>
        Seq(compressed.sql +#+ d"-> 0").funcall(d"st_geomfromgeojson")
    }
  }

  def sqlizeLocationPoint = ofs { (f, args, ctx) =>
    assert(args.length == 1)
    assert(args(0).typ == SoQLLocation)

    val pointSubcolumnSql = pointSqlFromLocationSql(args(0))

    ExprSql(pointSubcolumnSql, f)
  }

  // Like "sqlNormalOrdinaryFunction" but it extracts the point
  // subcolumn from any locations passed in.
  def sqlizeLocationPointOrdinaryFunction(sqlFunctionName: String, prefixArgs: Seq[Doc] = Nil, suffixArgs: Seq[Doc] = Nil) = {
    val funcName = Doc(sqlFunctionName)
    ofs { (f, args, ctx) =>
      assert(args.length >= f.function.minArity)
      assert(f.function.allParameters.startsWith(args.map(_.typ)))

      val pointExtractedArgs = args.map { e =>
        e.typ match {
          case SoQLLocation => pointSqlFromLocationSql(e)
          case _ => e.compressed.sql
        }
      }
      val sql = (prefixArgs ++ pointExtractedArgs ++ suffixArgs).funcall(funcName)

      ExprSql(sql.group, f)
    }
  }

  def textSqlAsJson(textColOrSubcolSql: Doc): Doc = {
    val jsonifiedSubcol = Seq(textColOrSubcolSql).funcall(d"to_jsonb") +#+ d":: text"
    Seq(jsonifiedSubcol, d"'null'").funcall(d"coalesce")
  }

  def textSqlsAsHumanAddressSql(textSqls: Seq[Doc]): Doc = {
    val Seq(addr, city, state, zip) = textSqls.map(textSqlAsJson)

    // Ugh ok.  If all the physical (sub)columns that make up this
    // "human address" are null, then the whole human address is null.
    val nullCheck = textSqls.
      map { s => (s.parenthesized +#+ d"is null").parenthesized }.
      reduceLeft { (l, r) => l +#+ d"and" +#+ r }.
      group

    val resultIfNotNull = d"""'{"address":' ||""" +#+ addr +#+ d"""|| ',"city":' ||""" +#+ city +#+ d"""|| ',"state":' ||""" +#+ state +#+ d"""|| ',"zip":' ||""" +#+ zip +#+ d"""|| '}'"""

    val clauses = Seq(
      ((d"WHEN" +#+ nullCheck).nest(2).group ++ Doc.lineSep ++ d"THEN NULL :: text").nest(2).group,
      (d"ELSE" +#+ resultIfNotNull).nest(2).group
    )

    val doc = (d"CASE" ++ Doc.lineSep ++ clauses.vsep).nest(2) ++ Doc.lineSep ++ d"END"

    doc.group
  }

  def sqlizeLocationHumanAddress = ofs { (f, args, ctx) =>
    assert(args.length == 1)
    assert(args(0).typ == SoQLLocation)
    assert(f.typ == SoQLText)

    val sqls = args(0) match {
      case expanded: ExprSql.Expanded[MT] =>
        expanded.sqls.drop(1)
      case compressed: ExprSql.Compressed[MT] =>
        (1 to 4).map { i => compressed.sql +#+ d"->>" +#+ Doc(i) }
    }

    ExprSql(textSqlsAsHumanAddressSql(sqls), f)
  }

  def sqlizeHumanAddress = ofs { (f, args, ctx) =>
    assert(args.length == 4)
    assert(args.forall(_.typ == SoQLText))
    assert(f.typ == SoQLText)
    ExprSql(textSqlsAsHumanAddressSql(args.map(_.compressed.sql)), f)
  }

  def sqlizeLocation = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLLocation)
    assert(args.length == 5)
    assert(args(0).typ == SoQLPoint)
    for(i <- 1 to 4) {
      assert(args(i).typ == SoQLText)
    }
    // We're given the five subcolumns that make up a fake-location,
    // so just pass them on through.
    ExprSql(args.map(_.compressed.sql), f)(ctx.repFor, ctx.gensymProvider)
  }

  def sqlizeSimpleCompoundColumn(typ: SoQLType) = ofs { (f, args, ctx) =>
    assert(f.typ == typ)
    assert(args.length == ctx.repFor(typ).expandedColumnCount)
    assert(args.forall(_.typ == SoQLText))
    // We're given both the subcolumns that make up a `typ`, so just
    // pass them on through.
    ExprSql(args.map(_.compressed.sql), f)(ctx.repFor, ctx.gensymProvider)
  }

  def sqlizeChr = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLText)
    assert(args.length == 1)
    assert(args(0).typ == SoQLNumber)

    ExprSql(
      Seq(args(0).compressed.sql +#+ d":: int").funcall(d"chr"),
      f
    )
  }

  def sqlizeSubstr(n: Int) = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLText)
    assert(args.length == 1 + n)
    assert(args(0).typ == SoQLText)
    assert(args.tail.forall(_.typ == SoQLNumber))

    ExprSql(
      (args(0).compressed.sql +: args.tail.map(_.compressed.sql +#+ d":: int")).funcall(d"substring"),
      f
    )
  }

  def sqlizeRound = ofs { (f, args, ctx) =>
    assert(args.length == 2)
    assert(args.forall(_.typ == SoQLNumber))
    assert(f.typ == SoQLNumber)

    val sql = Seq(
      args(0).compressed.sql,
      Seq(args(1).compressed.sql.parenthesized +#+ d"as int").funcall(d"cast")
    ).funcall(d"round")

    ExprSql(sql, f)
  }

  def sqlizeGetUtcDate = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLFixedTimestamp)
    assert(args.length == 0)

    ctx.nowUsed = true
    ctx.repFor(f.typ).
      literal(LiteralValue[MT](SoQLFixedTimestamp(ctx.now))(AtomicPositionInfo.None))(ctx.gensymProvider).
      withExpr(f)
  }

  def sqlizeIsEmpty = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLBoolean)
    assert(args.length == 1)

    val base = Seq(args(0).compressed.sql).funcall(d"st_isempty")

    // That our is_empty function has different null semantics
    // from st_isempty is a little weird and annoying.
    val sql = args(0).expr match {
      case _ : LiteralValue | _ : Column =>
        // this way we _might_ be able to use an index
        base +#+ d"or" +#+ args(0).compressed.sql.parenthesized +#+ d"is null"
      case _ =>
        // this keeps us from evaluating whatever the non-trivial expr
        // was more than once.
        Seq(base, d"true").funcall(d"coalesce")
    }

    ExprSql(sql, f)
  }

  def doSqlizeWithinCircle(f: Expr, scrutineeSql: Doc, lat: ExprSql, lon: ExprSql, radius: ExprSql): ExprSql = {
    assert(lat.typ == SoQLNumber)
    assert(lon.typ == SoQLNumber)
    assert(radius.typ == SoQLNumber)
    val sql = Seq(
      scrutineeSql,
      Seq(
        Seq(lon.compressed.sql, lat.compressed.sql).funcall(d"st_makepoint") +#+ d":: geography",
        radius.compressed.sql
      ).funcall(d"st_buffer") +#+ d":: geometry"
    ).funcall(d"st_within")

    ExprSql(sql, f)
  }

  def sqlizeWithinCircle = ofs { (f, args, ctx) =>
    val Seq(scrutinee, lat, lon, radius) = args
    doSqlizeWithinCircle(f, scrutinee.compressed.sql, lat, lon, radius)
  }

  def sqlizeLocationWithinCircle = ofs { (f, args, ctx) =>
    val Seq(scrutinee, lat, lon, radius) = args
    doSqlizeWithinCircle(f, pointSqlFromLocationSql(scrutinee), lat, lon, radius)
  }

  def doSqlizeWithinBox(f: Expr, scrutineeSql: Doc, topLeftLat: ExprSql, topLeftLon: ExprSql, bottomRightLat: ExprSql, bottomRightLon: ExprSql): ExprSql = {
    assert(topLeftLat.typ == SoQLNumber)
    assert(topLeftLon.typ == SoQLNumber)
    assert(bottomRightLat.typ == SoQLNumber)
    assert(bottomRightLon.typ == SoQLNumber)

    // st_makeenvelope takes (xmin, ymin, xmax, ymax, srid) so permute
    // our lat/lon based coords appropriately
    val sql = Seq(
      topLeftLon.compressed.sql,
      bottomRightLat.compressed.sql,
      bottomRightLon.compressed.sql,
      topLeftLat.compressed.sql,
      defaultSRIDLiteral
    ).funcall(d"st_makeenvelope") +#+ d"~" +#+ scrutineeSql.parenthesized

    ExprSql(sql, f)
  }

  def sqlizeWithinBox = ofs { (f, args, ctx) =>
    val Seq(scrutinee, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon) = args
    doSqlizeWithinBox(f, scrutinee.compressed.sql, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon)
  }

  def sqlizeLocationWithinBox = ofs { (f, args, ctx) =>
    val Seq(scrutinee, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon) = args
    doSqlizeWithinBox(f, pointSqlFromLocationSql(scrutinee), topLeftLat, topLeftLon, bottomRightLat, bottomRightLon)
  }

  def sqlizeArea = ofs { (f, args, ctx) =>
    // annoyingly this geography cast means this can't just be 'numericize(sqlizeOrdinary("st_area"))'
    assert(args.length == 1)

    val sql = Seq(
      args(0).compressed.sql.parenthesized +#+ d":: geography"
    ).funcall(d"st_area") +#+ d":: numeric"

    ExprSql(sql, f)
  }

  def doDistanceInMeters(f: Expr, aSql: Doc, bSql: Doc): ExprSql = {
    val sql = Seq(
      aSql.parenthesized +#+ d":: geography",
      bSql.parenthesized +#+ d":: geography"
    ).funcall(d"st_distance") +#+ d":: numeric"

    ExprSql(sql, f)
  }

  def sqlizeDistanceInMeters = ofs { (f, args, ctx) =>
    assert(args.length == 2)
    doDistanceInMeters(f, args(0).compressed.sql, args(1).compressed.sql)
  }

  def sqlizeLocationDistanceInMeters = ofs { (f, args, ctx) =>
    assert(args.length == 2)
    doDistanceInMeters(f, pointSqlFromLocationSql(args(0)), pointSqlFromLocationSql(args(1)))
  }

  // Given an ordinary function sqlizer, returns a new ordinary
  // function sqlizer that upcases all of its text arguments
  def uncased(sqlizer: OrdinaryFunctionSqlizer): OrdinaryFunctionSqlizer =
    ofs { (f, args, ctx) =>
      sqlizer(f, args.map(uncased), ctx)
    }
  def uncased(expr: ExprSql): ExprSql =
    expr.typ match {
      case SoQLText =>
        ExprSql(Seq(expr.compressed.sql).funcall(Doc("upper")).group, expr.expr)
      case _ =>
        expr
    }

  // if one of the args is a multi-geometry, wrap in st_multi (this is
  // used when a function can _sometimes_ turn a multithing into a
  // thing)
  def preserveMulti(sqlizer: OrdinaryFunctionSqlizer): OrdinaryFunctionSqlizer =
    ofs { (f, args, ctx) =>
      val exprSql = sqlizer(f, args, ctx)
      if(args.exists { arg => arg.typ == SoQLMultiPolygon || arg.typ == SoQLMultiLine || arg.typ == SoQLMultiPoint }) {
        ExprSql(Seq(exprSql.compressed.sql).funcall(d"st_multi"), f)
      } else {
        exprSql
      }
    }

  val ordinaryFunctionMap = (
    Seq[(Function[CT], OrdinaryFunctionSqlizer)](
      IsNull -> sqlizeIsNull,
      IsNotNull -> sqlizeIsNotNull,
      Not -> sqlizeUnaryOp("NOT"),

      Between -> sqlizeTrinaryOp("between", "and"),
      NotBetween -> sqlizeTrinaryOp("not between", "and"),

      In -> sqlizeInlike("IN"),
      CaselessOneOf -> uncased(sqlizeInlike("IN")),
      NotIn -> sqlizeInlike("NOT IN"),
      CaselessNotOneOf -> uncased(sqlizeInlike("NOT IN")),
      Eq -> sqlizeEq,
      EqEq -> sqlizeEq,
      CaselessEq -> uncased(sqlizeEq),
      Neq -> sqlizeNeq,
      BangEq -> sqlizeNeq,
      CaselessNe -> uncased(sqlizeNeq),
      And -> sqlizeBinaryOp("AND"),
      Or -> sqlizeBinaryOp("OR"),
      Lt -> sqlizeProvenancedBinaryOp("<"),
      Lte -> sqlizeProvenancedBinaryOp("<="),
      Gt -> sqlizeProvenancedBinaryOp(">"),
      Gte -> sqlizeProvenancedBinaryOp(">="),
      Least -> sqlizeNormalOrdinaryFuncall("least"),
      Greatest -> sqlizeNormalOrdinaryFuncall("greatest"),

      Like -> sqlizeBinaryOp("LIKE"),
      NotLike -> sqlizeBinaryOp("NOT LIKE"),
      Concat -> sqlizeBinaryOp("||"),
      Lower -> sqlizeNormalOrdinaryFuncall("lower"),
      Upper -> sqlizeNormalOrdinaryFuncall("upper"),
      Length -> sqlizeNormalOrdinaryFuncall("length"),
      Replace -> sqlizeNormalOrdinaryFuncall("replace"),
      Trim -> sqlizeNormalOrdinaryFuncall("trim"),
      TrimLeading -> sqlizeNormalOrdinaryFuncall("ltrim"),
      TrimTrailing -> sqlizeNormalOrdinaryFuncall("rtrim"),
      StartsWith -> sqlizeNormalOrdinaryFuncall("starts_with"),
      CaselessStartsWith -> uncased(sqlizeNormalOrdinaryFuncall("starts_with")),
      Contains -> sqlizeContains,
      CaselessContains -> uncased(sqlizeContains),
      LeftPad -> sqlizePad("lpad"),
      RightPad -> sqlizePad("rpad"),
      Chr -> sqlizeChr,
      Substr2 -> sqlizeSubstr(1),
      Substr3 -> sqlizeSubstr(2),

      UnaryPlus -> sqlizeUnaryOp("+"),
      UnaryMinus -> sqlizeUnaryOp("-"),
      BinaryPlus -> sqlizeBinaryOp("+"),
      BinaryMinus -> sqlizeBinaryOp("-"),
      TimesNumNum -> sqlizeBinaryOp("*"),
      TimesDoubleDouble -> sqlizeBinaryOp("*"),
      TimesMoneyNum -> sqlizeBinaryOp("*"),
      TimesNumMoney -> sqlizeBinaryOp("*"),
      DivNumNum -> sqlizeBinaryOp("/"),
      DivDoubleDouble -> sqlizeBinaryOp("/"),
      DivMoneyNum -> sqlizeBinaryOp("/"),
      DivMoneyMoney -> sqlizeBinaryOp("/"),
      ExpNumNum -> sqlizeBinaryOp("^"),
      ExpDoubleDouble -> sqlizeBinaryOp("^"),
      ModNumNum -> sqlizeBinaryOp("%"),
      ModDoubleDouble -> sqlizeBinaryOp("%"),
      ModMoneyNum -> sqlizeBinaryOp("%"),
      ModMoneyMoney -> sqlizeBinaryOp("%"),
      NaturalLog -> sqlizeNormalOrdinaryFuncall("ln"),
      Absolute -> sqlizeNormalOrdinaryFuncall("abs"),
      Ceiling -> sqlizeNormalOrdinaryFuncall("ceil"),
      Floor -> sqlizeNormalOrdinaryFuncall("floor"),
      Round -> sqlizeRound,
      WidthBucket -> numericize(sqlizeNormalOrdinaryFuncall("width_bucket")),

      // Timestamps
      ToFloatingTimestamp -> sqlizeBinaryOp("at time zone"),
      FloatingTimeStampTruncYmd -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'day'")),
      FloatingTimeStampTruncYm -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'")),
      FloatingTimeStampTruncY -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'")),
      FixedTimeStampZTruncYmd -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'day'")),
      FixedTimeStampZTruncYm -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'")),
      FixedTimeStampZTruncY -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'")),
      FloatingTimeStampExtractY -> sqlizeExtractTimestampField("year"),
      FloatingTimeStampExtractM -> sqlizeExtractTimestampField("month"),
      FloatingTimeStampExtractD -> sqlizeExtractTimestampField("day"),
      FloatingTimeStampExtractHh -> sqlizeExtractTimestampField("hour"),
      FloatingTimeStampExtractMm -> sqlizeExtractTimestampField("minute"),
      FloatingTimeStampExtractSs -> sqlizeExtractTimestampField("second"),
      FloatingTimeStampExtractDow -> sqlizeExtractTimestampField("dow"),
      FloatingTimeStampExtractWoy -> sqlizeExtractTimestampField("week"),
      FloatingTimestampExtractIsoY -> sqlizeExtractTimestampField("isoyear"),
      EpochSeconds -> sqlizeExtractTimestampField("epoch"),
      TimeStampDiffD -> sqlizeTimestampDiffD,
      TimeStampAdd -> sqlizeBinaryOp("+"),  // These two are exactly
      TimeStampPlus -> sqlizeBinaryOp("+"), // the same function??
      TimeStampMinus -> sqlizeBinaryOp("-"),
      GetUtcDate -> sqlizeGetUtcDate,

      // Geo-casts
      TextToPoint -> sqlizeGeomCast("st_pointfromtext"),
      TextToMultiPoint -> sqlizeGeomCast("st_mpointfromtext"),
      TextToLine -> sqlizeGeomCast("st_linefromtext"),
      TextToMultiLine -> sqlizeGeomCast("st_mlinefromtext"),
      TextToPolygon -> sqlizeGeomCast("st_polygonfromtext"),
      TextToMultiPolygon -> sqlizeGeomCast("st_mpolyfromtext"),

      // Geo
      Union2Pt -> sqlizeNormalOrdinaryWithWrapper("st_union", "st_multi"),
      Union2Line -> sqlizeNormalOrdinaryWithWrapper("st_union", "st_multi"),
      Union2Poly -> sqlizeNormalOrdinaryWithWrapper("st_union", "st_multi"),
      GeoMultiPolygonFromMultiPolygon -> sqlizeNormalOrdinaryFuncall("st_multi"),
      GeoMultiLineFromMultiLine -> sqlizeNormalOrdinaryFuncall("st_multi"),
      GeoMultiPointFromMultiPoint -> sqlizeNormalOrdinaryFuncall("st_multi"),
      GeoMultiPolygonFromPolygon -> sqlizeNormalOrdinaryFuncall("st_multi"),
      GeoMultiLineFromLine -> sqlizeNormalOrdinaryFuncall("st_multi"),
      GeoMultiPointFromPoint -> sqlizeNormalOrdinaryFuncall("st_multi"),
      NumberOfPoints -> sqlizeNormalOrdinaryFuncall("st_npoints"),
      PointToLatitude -> numericize(sqlizeNormalOrdinaryFuncall("st_y")),
      PointToLongitude -> numericize(sqlizeNormalOrdinaryFuncall("st_x")),
      NumberOfPoints -> numericize(sqlizeNormalOrdinaryFuncall("st_npoints")),
      Crosses -> sqlizeNormalOrdinaryFuncall("st_crosses"),
      Overlaps -> sqlizeNormalOrdinaryFuncall("st_overlaps"),
      Intersects -> sqlizeNormalOrdinaryFuncall("st_overlaps"),
      ReducePrecision -> sqlizeNormalOrdinaryFuncall("st_reduceprecision"),
      // https://postgis.net/docs/ST_ReducePrecision.html - Polygons
      // can become multipolygons when reduced, so we force them to do
      // so.
      ReducePolyPrecision -> sqlizeNormalOrdinaryWithWrapper("st_reduceprecision", "st_multi"),
      Intersection -> sqlizeMultiBuffered("st_intersection"),
      WithinPolygon -> sqlizeNormalOrdinaryFuncall("st_within"),
      WithinCircle -> sqlizeWithinCircle,
      WithinBox -> sqlizeWithinBox,
      IsEmpty -> sqlizeIsEmpty,
      Simplify -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_simplify")),
      SimplifyPreserveTopology -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_simplifypreservetopology")),
      SnapToGrid -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_snaptogrid")),
      Area -> sqlizeArea,
      DistanceInMeters -> sqlizeArea,

      ConcaveHull -> sqlizeMultiBuffered("st_concavehull"),
      ConvexHull -> sqlizeMultiBuffered("st_convexhull"),

      // Fake location
      LocationToLatitude -> numericize(sqlizeLocationPointOrdinaryFunction("st_y")),
      LocationToLongitude -> numericize(sqlizeLocationPointOrdinaryFunction("st_x")),
      LocationToAddress -> sqlizeLocationHumanAddress,
      LocationToPoint -> sqlizeLocationPoint,
      Location -> sqlizeLocation,
      HumanAddress -> sqlizeHumanAddress,
      LocationWithinPolygon -> sqlizeLocationPointOrdinaryFunction("st_within"),
      LocationWithinCircle -> sqlizeLocationWithinCircle,
      LocationWithinBox -> sqlizeLocationWithinBox,
      LocationDistanceInMeters -> sqlizeLocationDistanceInMeters,

      // URL
      UrlToUrl -> sqlizeSubcol(SoQLUrl, "url"),
      UrlToDescription -> sqlizeSubcol(SoQLUrl, "description"),
      Url -> sqlizeSimpleCompoundColumn(SoQLUrl),

      // Phone
      PhoneToPhoneNumber -> sqlizeSubcol(SoQLPhone, "phone_number"),
      PhoneToPhoneType -> sqlizeSubcol(SoQLPhone, "phone_type"),
      Phone -> sqlizeSimpleCompoundColumn(SoQLPhone),

      // json
      JsonProp -> sqlizeBinaryOp("->"),
      JsonIndex -> sqlizeBinaryOp("->"),
      TextToJson -> sqlizeCast("jsonb"),
      JsonToText -> sqlizeCast("text"),

      // conditional
      Nullif -> sqlizeNormalOrdinaryFuncall("nullif"),
      Coalesce -> sqlizeNormalOrdinaryFuncall("coalsece"),
      Case -> sqlizeCase,
      Iif -> sqlizeIif,

      // magical
      GetContext -> sqlizeGetContext,

      SoQLRewriteSearch.ToTsVector -> sqlizeNormalOrdinaryFuncall("to_tsvector"),
      SoQLRewriteSearch.ToTsQuery -> sqlizeNormalOrdinaryFuncall("to_tsquery"),
      SoQLRewriteSearch.TsSearch -> sqlizeBinaryOp("@@"),

      // simple casts
      TextToBool -> sqlizeCast("boolean"),
      BoolToText -> sqlizeCast("text"),
      TextToNumber -> sqlizeCast("numeric"),
      NumberToText -> sqlizeCast("text")
    ) ++ castIdentities.map { f =>
      f -> sqlizeIdentity _
    }
  ).map { case (f, sqlizer) =>
    f.identity -> sqlizer
  }.toMap

  // count_distinct is a separate function for legacy reasons; rewrite it into count(distinct ...)
  def sqlizeCountDistinct(e: AggregateFunctionCall, args: Seq[ExprSql], filter: Option[ExprSql], ctx: DynamicContext) = {
    sqlizeNormalAggregateFuncall("count")(e.copy(distinct = true)(e.position), args, filter, ctx)
  }

  def sqlizeMedianAgg(aggFunc: String, percentileFunc: String) = {
    val aggFuncSqlizer = sqlizeNormalAggregateFuncall(aggFunc)
    val percentileFuncName = Doc(percentileFunc)
    afs { (f, args, filter, ctx) =>
      if(f.distinct) {
        aggFuncSqlizer(f, args, filter, ctx)
      } else { // this is faster but there's no way to specify "distinct" with it
        val baseSql =
          (percentileFuncName ++ d"(.50) within group (" ++ Doc.lineCat ++ d"order by" +#+ args(0).compressed.sql).nest(2) ++ Doc.lineCat ++ d")"
        ExprSql(baseSql ++ sqlizeFilter(filter), f)
      }
    }
  }

  val aggregateFunctionMap = (
    Seq[(Function[CT], AggregateFunctionSqlizer)](
      Max -> sqlizeNormalAggregateFuncall("max", jsonbWorkaround = true),
      Min -> sqlizeNormalAggregateFuncall("min", jsonbWorkaround = true),
      CountStar -> numericize(sqlizeCountStar _),
      Count -> numericize(sqlizeNormalAggregateFuncall("count")),
      CountDistinct -> numericize(sqlizeCountDistinct _),
      Sum -> sqlizeNormalAggregateFuncall("sum"),
      Avg -> sqlizeNormalAggregateFuncall("avg"),
      Median -> sqlizeMedianAgg("median_ulib_agg", "percentile_cont"),
      MedianDisc -> sqlizeMedianAgg("median_disc_ulib_agg", "percentile_disc"),
      RegrIntercept -> sqlizeNormalAggregateFuncall("regr_intercept"),
      RegrR2 -> sqlizeNormalAggregateFuncall("regr_r2"),
      RegrSlope -> sqlizeNormalAggregateFuncall("regr_slope"),
      StddevPop -> sqlizeNormalAggregateFuncall("stddev_pop"),
      StddevSamp -> sqlizeNormalAggregateFuncall("stddev_samp"),

      UnionAggPt -> sqlizeNormalAggregateWithWrapper("st_union", "st_multi"),
      UnionAggLine -> sqlizeNormalAggregateWithWrapper("st_union", "st_multi"),
      UnionAggPoly -> sqlizeNormalAggregateWithWrapper("st_union", "st_multi"),
      Extent -> sqlizeNormalAggregateWithWrapper("st_extent", "st_multi")
    )
  ).map { case (f, sqlizer) =>
    f.identity -> sqlizer
  }.toMap

  def sqlizeLeadLag(name: String) = {
    val sqlizer = sqlizeNormalWindowedFuncall(name)

    wfs { (e, args, filter, partitionBy, orderBy, ctx) =>
      val mungedArgs = args.zipWithIndex.map { case (arg, i) =>
        if(i == 1) {
          assert(arg.typ == SoQLNumber)
          // Bit of a lie; this is no longer actually a SoQLNumber,
          // but we'll just be passing it into lead or lag, so it's
          // fine
          ExprSql(d"(" ++ arg.compressed.sql ++ d") :: int", arg.expr)
        } else {
          arg
        }
      }
      sqlizer(e, mungedArgs, filter, partitionBy, orderBy, ctx)
    }
  }

  val windowedFunctionMap = (
    Seq[(Function[CT], WindowedFunctionSqlizer)](
      RowNumber -> sqlizeNormalWindowedFuncall("row_number"),
      Rank -> sqlizeNormalWindowedFuncall("rank"),
      DenseRank -> sqlizeNormalWindowedFuncall("dense_rank"),
      FirstValue -> sqlizeNormalWindowedFuncall("first_value"),
      LastValue -> sqlizeNormalWindowedFuncall("last_value"),
      Lead -> sqlizeLeadLag("lead"),
      LeadOffset -> sqlizeLeadLag("lead"),
      LeadOffsetDefault -> sqlizeLeadLag("lead"),
      Lag -> sqlizeLeadLag("lag"),
      LagOffset -> sqlizeLeadLag("lag"),
      LagOffsetDefault -> sqlizeLeadLag("lag"),
      Ntile -> sqlizeLeadLag("ntile"),

      // aggregate functions, used in a windowed way
      Max -> sqlizeNormalWindowedFuncall("max", jsonbWorkaround = true),
      Min -> sqlizeNormalWindowedFuncall("min", jsonbWorkaround = true),
      CountStar -> numericize(sqlizeCountStarWindowed _),
      Count -> numericize(sqlizeNormalWindowedFuncall("count")),
      // count distinct is not an aggregatable function
      Sum -> sqlizeNormalWindowedFuncall("sum"),
      Avg -> sqlizeNormalWindowedFuncall("avg"),
      Median -> sqlizeNormalWindowedFuncall("median_ulib_agg"),          // have to use the custom aggregate function
      MedianDisc -> sqlizeNormalWindowedFuncall("median_disc_ulib_agg"), // when in a windowed context
      RegrIntercept -> sqlizeNormalWindowedFuncall("regr_intercept"),
      RegrR2 -> sqlizeNormalWindowedFuncall("regr_r2"),
      RegrSlope -> sqlizeNormalWindowedFuncall("regr_slope"),
      StddevPop -> sqlizeNormalWindowedFuncall("stddev_pop"),
      StddevSamp -> sqlizeNormalWindowedFuncall("stddev_samp"),

      UnionAggPt -> sqlizeNormalWindowedWithWrapper("st_union", "st_multi"),
      UnionAggLine -> sqlizeNormalWindowedWithWrapper("st_union", "st_multi"),
      UnionAggPoly -> sqlizeNormalWindowedWithWrapper("st_union", "st_multi"),
      Extent -> sqlizeNormalWindowedWithWrapper("st_extent", "st_multi")
    )
  ).map { case (f, sqlizer) =>
    f.identity -> sqlizer
  }.toMap

  override def sqlizeOrdinaryFunction(
    e: FunctionCall,
    args: Seq[ExprSql],
    ctx: DynamicContext
  ): ExprSql = {
    assert(!e.function.isAggregate)
    assert(!e.function.needsWindow)
    ordinaryFunctionMap(e.function.function.identity)(e, args, ctx)
  }

  override def sqlizeAggregateFunction(
    e: AggregateFunctionCall,
    args: Seq[ExprSql],
    filter: Option[ExprSql],
    ctx: DynamicContext
  ): ExprSql = {
    assert(e.function.isAggregate)
    assert(!e.function.needsWindow)
    aggregateFunctionMap(e.function.function.identity)(e, args, filter, ctx)
  }

  def sqlizeWindowedFunction(
    e: WindowedFunctionCall,
    args: Seq[ExprSql],
    filter: Option[ExprSql],
    partitionBy: Seq[ExprSql],
    orderBy: Seq[OrderBySql],
    ctx: DynamicContext
  ): ExprSql = {
    // either the function is a window function or it's an aggregate
    // function being used in a windowed way.
    assert(e.function.needsWindow || e.function.isAggregate)
    windowedFunctionMap(e.function.function.identity)(e, args, filter, partitionBy, orderBy, ctx)
  }
}
