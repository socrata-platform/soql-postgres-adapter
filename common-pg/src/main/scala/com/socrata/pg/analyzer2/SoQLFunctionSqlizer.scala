package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.NonEmptySeq
import com.socrata.soql.types._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions.{Function, MonomorphicFunction, SoQLTypeInfo}
import com.socrata.soql.sqlizer._

class SoQLFunctionSqlizer[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })] extends FuncallSqlizer[MT] {
  import SoQLTypeInfo.hasType

  override val exprSqlFactory = new SoQLExprSqlFactory[MT]

  def wrap(e: Expr, exprSql: ExprSql, wrapper: String, additionalWrapperArgs: Doc*) =
    exprSqlFactory((exprSql.compressed.sql +: additionalWrapperArgs).funcall(Doc(wrapper)), e)

  def numericize(sqlizer: OrdinaryFunctionSqlizer) = ofs { (f, args, ctx) =>
    val e = sqlizer(f, args, ctx)
    assert(e.typ == SoQLNumber)
    exprSqlFactory(e.compressed.sql.parenthesized +#+ d":: numeric", f)
  }

  def numericize(sqlizer: AggregateFunctionSqlizer) = afs { (f, args, filter, ctx) =>
    val e = sqlizer(f, args, filter, ctx)
    assert(e.typ == SoQLNumber)
    exprSqlFactory(e.compressed.sql.parenthesized +#+ d":: numeric", f)
  }

  def numericize(sqlizer: WindowedFunctionSqlizer) = wfs { (f, args, filter, partitionBy, orderBy, ctx) =>
    val e = sqlizer(f, args, filter, partitionBy, orderBy, ctx)
    assert(e.typ == SoQLNumber)
    exprSqlFactory(e.compressed.sql.parenthesized +#+ d":: numeric", f)
  }

  def dateify(sqlizer: OrdinaryFunctionSqlizer) = ofs { (f, args, ctx) =>
    val e = sqlizer(f, args, ctx)
    assert(e.typ == SoQLDate)
    exprSqlFactory(e.compressed.sql.parenthesized +#+ d":: date", f)
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

    val sql = (args.map(_.compressed.sql) :+ Geo.defaultSRIDLiteral).funcall(Doc(sqlFunctionName))

    exprSqlFactory(sql.group, f)
  }

  def sqlizeCase = ofs { (f, args, ctx) =>
    assert(f.function.function eq Case)
    assert(args.length % 2 == 0)
    assert(args.length >= 2)

    val lastCase = args.takeRight(2)

    def sqlizeCondition(conditionConsequent: Seq[ExprSql]) = {
      val Seq(condition, consequent) = conditionConsequent
      new CaseClause(condition.compressed.sql, consequent.compressed.sql)
    }

    val caseBuilder =
      lastCase.head.expr match {
        case LiteralValue(SoQLBoolean(true)) if args.length > 2 =>
          val initialCases = args.dropRight(2)
          val otherwise = lastCase(1)
          Right(new CaseBuilder(
            NonEmptySeq.fromSeq(initialCases.grouped(2).map(sqlizeCondition).toSeq).getOrElse {
              throw new Exception("NonEmptySeq failed but I've checked that there are enough cases")
            },
            Some(new ElseClause(otherwise.compressed.sql))
          ))
        case LiteralValue(SoQLBoolean(true)) if args.length == 2 =>
          // We just have a single clause and the guard on it is a
          // constant true, so just return the clause's consequent.
          // This isn't a useful optimization in the real world, but
          // the tests use `case when true then something end` to
          // force "something" to be compressed.
          Left(args(1).compressed)
        case _ =>
          Right(new CaseBuilder(
            NonEmptySeq.fromSeq(args.grouped(2).map(sqlizeCondition).toSeq).getOrElse {
              throw new Exception("NonEmptySeq failed but I've checked that there are enough cases")
            },
            None
          ))
      }

    caseBuilder match {
      case Right(actualCaseBuilder) =>
        exprSqlFactory(actualCaseBuilder.sql, f)
      case Left(exprSql) =>
        exprSql
    }
  }

  def sqlizeIif = ofs { (f, args, ctx) =>
    assert(f.function.function eq Iif)
    assert(args.length == 3)

    val sql = caseBuilder(args(0).compressed.sql -> args(1).compressed.sql).withElse(args(2).compressed.sql)

    exprSqlFactory(sql.sql, f)
  }

  def sqlizeGetContext = ofs { (f, args, ctx) =>
    // ok, args have already been sqlized if we want to use them, but
    // we only want to use them if it wasn't a literal value.
    assert(f.function.function eq GetContext)
    assert(args.length == 1)
    assert(f.args.length == 1)
    assert(f.typ == SoQLText)

    def nullLiteral =
      ctx.repFor(SoQLText).nullLiteral(NullLiteral[MT](SoQLText)(f.position.asAtomic))
        .withExpr(f)

    f.args(0) match {
      case lit@LiteralValue(SoQLText(key)) =>
        ctx.extraContext.systemContextKeyUsed(key) match {
          case Some(value) =>
            ctx.repFor(SoQLText).literal(LiteralValue[MT](SoQLText(value))(f.position.asAtomic))
              .withExpr(f)
          case None =>
            nullLiteral
        }
      case e@NullLiteral(typ) =>
        nullLiteral
      case _ =>
        ctx.extraContext.nonLiteralSystemContextUsed()
        val lookup = Seq(args(0).compressed.sql).funcall(d"pg_temp.dynamic_system_context")
        exprSqlFactory(lookup, f)
    }
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
          case SoQLLocation => ctx.repFor(SoQLLocation).subcolInfo("point").extractor(e)
          case _ => e.compressed.sql
        }
      }
      val sql = (prefixArgs ++ pointExtractedArgs ++ suffixArgs).funcall(funcName)

      exprSqlFactory(sql.group, f)
    }
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

    exprSqlFactory(sqls.funcall(d"soql_human_address"), f)
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
    exprSqlFactory(args.map(_.compressed.sql), f)
  }

  def sqlizeSimpleCompoundColumn(typ: SoQLType) = ofs { (f, args, ctx) =>
    assert(f.typ == typ)
    assert(args.length == ctx.repFor(typ).expandedColumnCount)
    assert(args.forall(_.typ == SoQLText))
    // We're given all the subcolumns that make up a `typ`, so just
    // pass them on through.
    exprSqlFactory(args.map(_.compressed.sql), f)
  }

  def sqlizeGetUtcDate = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLFixedTimestamp)
    assert(args.length == 0)

    ctx.repFor(f.typ).
      literal(LiteralValue[MT](SoQLFixedTimestamp(ctx.extraContext.timestampProvider.now))(AtomicPositionInfo.Synthetic)).
      withExpr(f)
  }

  def convertToText(v: ExprSql, ctx: DynamicContext): ExprSql = {
    val rep = ctx.repFor(v.typ)
    ctx.extraContext.obfuscatorRequired ||= rep.isProvenanced
    rep.convertToText(v).getOrElse {
      throw new Exception("All types should be convertable to text, but ${v.typ} wasn't?")
    }
  }

  def sqlizeConcat = {
    val concat = sqlizeBinaryOp("||")
    ofs { (f, args, ctx) =>
      concat(f, args.map(convertToText(_, ctx)), ctx)
    }
  }

  def sqlizeCastToText = ofs { (f, args, ctx) =>
    assert(f.typ == SoQLText)
    assert(args.length == 1)
    convertToText(args(0), ctx).withExpr(f)
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

    exprSqlFactory(sql, f)
  }

  def sqlizeNegate = {
    val base = sqlizeUnaryOp("-")

    ofs { (f, args, ctx) =>
      assert(args.length == 1)

      args(0).expr match {
        case lit@LiteralValue(SoQLNumber(n)) =>
          // move the negation into the literal
          ctx.repFor(SoQLNumber).literal(LiteralValue[MT](SoQLNumber(n.negate))(lit.position)).withExpr(f)
        case _ =>
          base(f, args, ctx)
      }
    }
  }

  def sqlizeAntinegate = ofs { (f, args, ctx) =>
    assert(args.length == 1)
    // this operator only exists for symmetry with unary -, so just
    // get rid of it
    args(0)
  }

  def sqlizeExtractDateSubfield(field: Doc) = ofs { (f, args, ctx) =>
    assert(args.length == 1)
    assert(args(0).typ == SoQLFloatingTimestamp || args(0).typ == SoQLDate || args(0).typ == SoQLTime)
    assert(f.typ == SoQLNumber)

    // EXTRACT returns numeric; there's no need to cast to keep soqlnumber's representation correct
    val sql = Seq(field +#+ d"from" +#+ args(0).compressed.sql.parenthesized).funcall(d"extract")
    exprSqlFactory(sql, f)
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
        exprSqlFactory(Seq(expr.compressed.sql).funcall(Doc("upper")).group, expr.expr)
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
        exprSqlFactory(Seq(exprSql.compressed.sql).funcall(d"st_multi"), f)
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
      Concat -> sqlizeConcat,
      Lower -> sqlizeNormalOrdinaryFuncall("lower"),
      Upper -> sqlizeNormalOrdinaryFuncall("upper"),
      Length -> sqlizeNormalOrdinaryFuncall("length"),
      Replace -> sqlizeNormalOrdinaryFuncall("replace"),
      Trim -> sqlizeNormalOrdinaryFuncall("trim"),
      TrimLeading -> sqlizeNormalOrdinaryFuncall("ltrim"),
      TrimTrailing -> sqlizeNormalOrdinaryFuncall("rtrim"),
      StartsWith -> sqlizeNormalOrdinaryFuncall("starts_with"),
      CaselessStartsWith -> uncased(sqlizeNormalOrdinaryFuncall("starts_with")),
      Contains -> sqlizeNormalOrdinaryFuncall("soql_contains"),
      CaselessContains -> uncased(sqlizeNormalOrdinaryFuncall("soql_contains")),
      LeftPad -> sqlizeNormalOrdinaryFuncall("soql_left_pad"),
      RightPad -> sqlizeNormalOrdinaryFuncall("soql_right_pad"),
      Chr -> sqlizeNormalOrdinaryFuncall("soql_chr"),
      Substr2 -> sqlizeNormalOrdinaryFuncall("soql_substring"),
      Substr3 -> sqlizeNormalOrdinaryFuncall("soql_substring"),
      SplitPart -> sqlizeNormalOrdinaryFuncall("soql_split_part"),
      Unaccent -> sqlizeNormalOrdinaryFuncall("unaccent"),

      UnaryMinus -> sqlizeNegate,
      UnaryPlus -> sqlizeAntinegate,
      BinaryPlus -> sqlizeBinaryOp("+"),
      BinaryMinus -> sqlizeBinaryOp("-"),
      TimesNumNum -> sqlizeBinaryOp("*"),
      TimesDoubleDouble -> sqlizeBinaryOp("*"),
      DivNumNum -> sqlizeBinaryOp("/"),
      DivDoubleDouble -> sqlizeBinaryOp("/"),
      ExpNumNum -> sqlizeBinaryOp("^"),
      ExpDoubleDouble -> sqlizeBinaryOp("^"),
      ModNumNum -> sqlizeBinaryOp("%"),
      ModDoubleDouble -> sqlizeBinaryOp("%"),
      NaturalLog -> sqlizeNormalOrdinaryFuncall("ln"),
      Absolute -> sqlizeNormalOrdinaryFuncall("abs"),
      Ceiling -> sqlizeNormalOrdinaryFuncall("ceil"),
      Floor -> sqlizeNormalOrdinaryFuncall("floor"),
      Round -> sqlizeNormalOrdinaryFuncall("soql_round"),
      WidthBucket -> numericize(sqlizeNormalOrdinaryFuncall("width_bucket")),
      SignedMagnitude10 -> sqlizeNormalOrdinaryFuncall("soql_signed_magnitude_10"),
      SignedMagnitudeLinear -> sqlizeNormalOrdinaryFuncall("soql_signed_magnitude_linear"),

      // Timestamps
      ToFloatingTimestamp -> sqlizeBinaryOp("at time zone"),
      FloatingTimeStampTruncYmd -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'day'")),
      FloatingTimeStampTruncYm -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'")),
      FloatingTimeStampTruncY -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'")),
      DateTruncYm -> dateify(sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'"))),
      DateTruncY -> dateify(sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'"))),
      FixedTimeStampZTruncYmd -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'day'")),
      FixedTimeStampZTruncYm -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'")),
      FixedTimeStampZTruncY -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'")),
      FixedTimeStampTruncYmdAtTimeZone -> sqlizeNormalOrdinaryFuncall("soql_trunc_fixed_timestamp_at_timezone", prefixArgs = Seq(d"'day'")),
      FixedTimeStampTruncYmAtTimeZone -> sqlizeNormalOrdinaryFuncall("soql_trunc_fixed_timestamp_at_timezone", prefixArgs = Seq(d"'month'")),
      FixedTimeStampTruncYAtTimeZone -> sqlizeNormalOrdinaryFuncall("soql_trunc_fixed_timestamp_at_timezone", prefixArgs = Seq(d"'year'")),
      FloatingTimeStampExtractY -> sqlizeExtractDateSubfield(d"year"),
      FloatingTimestampYearField -> sqlizeExtractDateSubfield(d"year"),
      DateYearField -> sqlizeExtractDateSubfield(d"year"),
      FloatingTimeStampExtractM -> sqlizeExtractDateSubfield(d"month"),
      FloatingTimestampMonthField -> sqlizeExtractDateSubfield(d"month"),
      DateMonthField -> sqlizeExtractDateSubfield(d"month"),
      FloatingTimeStampExtractD -> sqlizeExtractDateSubfield(d"day"),
      FloatingTimestampDayField -> sqlizeExtractDateSubfield(d"day"),
      DateDayField -> sqlizeExtractDateSubfield(d"day"),
      FloatingTimeStampExtractHh -> sqlizeExtractDateSubfield(d"hour"),
      FloatingTimestampHourField -> sqlizeExtractDateSubfield(d"hour"),
      TimeHourField -> sqlizeExtractDateSubfield(d"hour"),
      FloatingTimeStampExtractMm -> sqlizeExtractDateSubfield(d"minute"),
      FloatingTimestampMinuteField -> sqlizeExtractDateSubfield(d"minute"),
      TimeMinuteField -> sqlizeExtractDateSubfield(d"minute"),
      FloatingTimeStampExtractSs -> sqlizeExtractDateSubfield(d"second"),
      FloatingTimestampSecondField -> sqlizeExtractDateSubfield(d"second"),
      TimeSecondField -> sqlizeExtractDateSubfield(d"seoncd"),
      FloatingTimeStampExtractDow -> sqlizeExtractDateSubfield(d"dow"),
      FloatingTimestampDayOfWeekField -> sqlizeExtractDateSubfield(d"dow"),
      DateDayOfWeekField -> sqlizeExtractDateSubfield(d"dow"),
      FloatingTimeStampExtractWoy -> sqlizeExtractDateSubfield(d"week"),
      FloatingTimestampWeekOfYearField -> sqlizeExtractDateSubfield(d"week"),
      DateWeekOfYearField -> sqlizeExtractDateSubfield(d"week"),
      FloatingTimestampExtractIsoY -> sqlizeExtractDateSubfield(d"isoyear"),
      FloatingTimestampIsoYearField -> sqlizeExtractDateSubfield(d"isoyear"),
      DateIsoYearField -> sqlizeExtractDateSubfield(d"isoyear"),
      FloatingTimestampDateField -> sqlizeCast("date"),
      FloatingTimestampTimeField -> sqlizeCast("time without time zone"),
      EpochSeconds -> sqlizeNormalOrdinaryFuncall("soql_epoch_seconds"),
      TimeStampDiffD -> sqlizeNormalOrdinaryFuncall("soql_timestamp_diff_d"),
      FixedTimeStampDiffBusinessDays -> sqlizeNormalOrdinaryFuncall("soql_diff_business_days"),
      FloatingTimeStampDiffBusinessDays -> sqlizeNormalOrdinaryFuncall("soql_diff_business_days"),
      TimeStampAdd -> sqlizeBinaryOp("+"),  // These two are exactly
      TimeStampPlus -> sqlizeBinaryOp("+"), // the same function??
      DateTimeAdd -> sqlizeBinaryOp("+"),
      TimeDateAdd -> sqlizeBinaryOp("+"),
      TimeIntervalAdd -> sqlizeBinaryOp("+"),
      IntervalTimeAdd -> sqlizeBinaryOp("+"),
      TimeIntervalSub -> sqlizeBinaryOp("-"),
      TimeTimeSub -> sqlizeBinaryOp("-"),
      DateIntervalAdd -> sqlizeBinaryOp("+"),
      IntervalDateAdd -> sqlizeBinaryOp("+"),
      DateIntervalSub -> sqlizeBinaryOp("-"),
      DateDiffD -> numericize(sqlizeBinaryOp("-")),
      DateDiffBusinessDays -> sqlizeNormalOrdinaryFuncall("soql_diff_business_days"),
      DateDateSub -> numericize(sqlizeBinaryOp("-")),
      TimeStampMinus -> sqlizeBinaryOp("-"),
      GetUtcDate -> sqlizeGetUtcDate,

      // Geo-casts
      TextToPoint -> sqlizeGeomCast("st_pointfromtext"),
      TextToMultiPoint -> sqlizeGeomCast("st_mpointfromtext"),
      TextToLine -> sqlizeGeomCast("st_linefromtext"),
      TextToMultiLine -> sqlizeGeomCast("st_mlinefromtext"),
      TextToPolygon -> sqlizeGeomCast("st_polygonfromtext"),
      TextToMultiPolygon -> sqlizeGeomCast("st_mpolyfromtext"),
      TextToLocation -> sqlizeNormalOrdinaryFuncall("soql_text_to_location", suffixArgs = Seq(Geo.defaultSRIDLiteral)),

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
      PointToLatitude -> numericize(sqlizeNormalOrdinaryFuncall("st_y")),
      PointToLongitude -> numericize(sqlizeNormalOrdinaryFuncall("st_x")),
      MakePoint -> sqlizeNormalOrdinaryFuncall("soql_make_point", suffixArgs = Seq(Geo.defaultSRIDLiteral)),
      NumberOfPoints -> numericize(sqlizeNormalOrdinaryFuncall("st_npoints")),
      Crosses -> sqlizeNormalOrdinaryFuncall("st_crosses"),
      Overlaps -> sqlizeNormalOrdinaryFuncall("st_overlaps"),
      Intersects -> sqlizeNormalOrdinaryFuncall("st_intersects"),
      ReducePrecision -> sqlizeNormalOrdinaryFuncall("st_reduceprecision"),
      // https://postgis.net/docs/ST_ReducePrecision.html - Polygons
      // can become multipolygons when reduced, so we force them to do
      // so.
      ReducePolyPrecision -> sqlizeNormalOrdinaryWithWrapper("st_reduceprecision", "st_multi"),
      Intersection -> sqlizeMultiBuffered("st_intersection"),
      WithinPolygon -> sqlizeNormalOrdinaryFuncall("st_within"),
      WithinCircle -> sqlizeNormalOrdinaryFuncall("soql_within_circle", suffixArgs = Seq(Geo.defaultSRIDLiteral)),
      WithinBox -> sqlizeNormalOrdinaryFuncall("soql_within_box", suffixArgs = Seq(Geo.defaultSRIDLiteral)),
      IsEmpty -> sqlizeIsEmpty,
      Simplify -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_simplify")),
      SimplifyPreserveTopology -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_simplifypreservetopology")),
      SnapToGrid -> preserveMulti(sqlizeNormalOrdinaryFuncall("st_snaptogrid")),
      Area -> sqlizeNormalOrdinaryFuncall("soql_area"),
      DistanceInMeters -> sqlizeNormalOrdinaryFuncall("soql_distance_in_meters"),
      VisibleAt -> sqlizeNormalOrdinaryFuncall("soql_visible_at"),
      GeoMakeValid -> sqlizeNormalOrdinaryFuncall("st_makevalid"),
      ConcaveHull -> sqlizeMultiBuffered("st_concavehull"),
      ConvexHull -> sqlizeMultiBuffered("st_convexhull"),
      CuratedRegionTest -> sqlizeNormalOrdinaryFuncall("soql_curated_region_test"),

      // ST_CollectionExtract takes a type as a second argument
      // See https://postgis.net/docs/ST_CollectionExtract.html for exact integer -> type mapping
      // (these are weird, you shouldn't ever have a "collection" value in soql...)
      GeoCollectionExtractMultiPolygonFromPolygon -> sqlizeNormalOrdinaryFuncall("st_collectionextract", suffixArgs = Seq(d"3")),
      GeoCollectionExtractMultiLineFromLine -> sqlizeNormalOrdinaryFuncall("st_collectionextract", suffixArgs = Seq(d"2")),
      GeoCollectionExtractMultiPointFromPoint -> sqlizeNormalOrdinaryFuncall("st_collectionextract", suffixArgs = Seq(d"1")),

      // Fake location
      LocationToLatitude -> numericize(sqlizeLocationPointOrdinaryFunction("st_y")),
      LocationToLongitude -> numericize(sqlizeLocationPointOrdinaryFunction("st_x")),
      LocationToAddress -> sqlizeLocationHumanAddress,
      LocationToPoint -> sqlizeSubcol(SoQLLocation, "point"),
      Location -> sqlizeLocation,
      HumanAddress -> sqlizeNormalOrdinaryFuncall("soql_human_address"),
      LocationWithinPolygon -> sqlizeLocationPointOrdinaryFunction("st_within"),
      LocationWithinCircle -> sqlizeLocationPointOrdinaryFunction("soql_within_circle", suffixArgs = Seq(Geo.defaultSRIDLiteral)),
      LocationWithinBox -> sqlizeLocationPointOrdinaryFunction("soql_within_box", suffixArgs = Seq(Geo.defaultSRIDLiteral)),
      LocationDistanceInMeters -> sqlizeLocationPointOrdinaryFunction("soql_distance_in_meters"),

      // URL
      UrlToUrl -> sqlizeSubcol(SoQLUrl, "url"),
      UrlToDescription -> sqlizeSubcol(SoQLUrl, "description"),
      Url -> sqlizeSimpleCompoundColumn(SoQLUrl),

      // Document
      DocumentToFilename  -> sqlizeJsonSubcol(SoQLDocument, "'filename'", SoQLText),
      DocumentToFileId -> sqlizeJsonSubcol(SoQLDocument, "'file_id'", SoQLText),
      DocumentToContentType -> sqlizeJsonSubcol(SoQLDocument, "'content_type'", SoQLText),

      // json
      JsonProp -> sqlizeBinaryOp("->"),
      JsonIndex -> sqlizeBinaryOp("->"),
      TextToJson -> sqlizeCast("jsonb"),
      JsonToText -> sqlizeCastToText,

      // conditional
      Nullif -> sqlizeNormalOrdinaryFuncall("nullif"),
      Coalesce -> sqlizeNormalOrdinaryFuncall("coalesce"),
      Case -> sqlizeCase,
      Iif -> sqlizeIif,

      // magical
      GetContext -> sqlizeGetContext,

      SoQLRewriteSearch.ToTsVector -> sqlizeNormalOrdinaryFuncall("to_tsvector", prefixArgs = Seq(d"'english'")),
      SoQLRewriteSearch.PlainToTsQuery -> sqlizeNormalOrdinaryFuncall("plainto_tsquery", prefixArgs = Seq(d"'english'")),
      SoQLRewriteSearch.TsSearch -> sqlizeBinaryOp("@@"),

      // simple casts
      RowIdentifierToText -> sqlizeCastToText,
      RowVersionToText -> sqlizeCastToText,
      TextToBool -> sqlizeCast("boolean"),
      BoolToText -> sqlizeCastToText,
      TextToNumber -> sqlizeCast("numeric"),
      NumberToText -> sqlizeCastToText,
      NumberToDouble -> sqlizeCast("double precision"),
      FixedTimestampToText -> sqlizeCastToText,
      TextToFixedTimestamp -> sqlizeCast("timestamp with time zone"),
      FloatingTimestampToText -> sqlizeCastToText,
      TextToFloatingTimestamp -> sqlizeCast("timestamp without time zone"),
      DateToText -> sqlizeCastToText,
      TimeToText -> sqlizeCastToText,
      TextToInterval -> sqlizeCast("interval"),
      IntervalToText -> sqlizeCastToText,
      TextToBlob -> sqlizeTypechangingIdentityCast,
      TextToPhoto -> sqlizeTypechangingIdentityCast
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
          ((percentileFuncName ++ d"(.50) within group (" ++ Doc.lineCat ++ d"order by" +#+ args(0).compressed.sql).nest(2) ++ Doc.lineCat ++ d")").parenthesized +#+ d"::" +#+ ctx.repFor(args(0).typ).compressedDatabaseType
        exprSqlFactory(baseSql ++ sqlizeFilter(filter), f)
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
          exprSqlFactory(d"(" ++ arg.compressed.sql ++ d") :: int", arg.expr)
        } else {
          arg
        }
      }
      sqlizer(e, mungedArgs, filter, partitionBy, orderBy, ctx)
    }
  }

  def sqlizeNtile(name: String) = {
    val sqlizer = sqlizeNormalWindowedFuncall(name)

    wfs { (e, args, filter, partitionBy, orderBy, ctx) =>
      val mungedArgs = args.zipWithIndex.map { case (arg, i) =>
        if (i == 0) {
          assert(arg.typ == SoQLNumber)
          exprSqlFactory(d"(" ++ arg.compressed.sql ++ d") :: int", arg.expr)
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
      Ntile -> sqlizeNtile("ntile"),

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
