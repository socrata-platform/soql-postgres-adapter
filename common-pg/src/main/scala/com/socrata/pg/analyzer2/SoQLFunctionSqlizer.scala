package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions.{Function, SoQLTypeInfo}

class SoQLFunctionSqlizer[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })] extends FuncallSqlizer[MT] {
  import SoQLTypeInfo.hasType

  def wrap(e: Expr, exprSql: ExprSql, wrapper: String, additionalWrapperArgs: Doc*) =
    ExprSql((exprSql.compressed.sql +: additionalWrapperArgs).funcall(Doc(wrapper)), e)

  def sqlizeNormalOrdinaryWithWrapper(name: String, wrapper: String) = ofs { (f, args, ctx) =>
    val exprSql = sqlizeNormalOrdinaryFuncall(name)(f, args, ctx)
    wrap(f, exprSql, wrapper)
  }

  def sqlizeNormalAggregateWithWrapper(name: String, wrapper: String) = afs { (f, args, filter, ctx) =>
    val exprSql = sqlizeNormalAggregateFuncall(name)(f, args, filter, ctx)
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

    val sql = (args.map(_.compressed.sql) :+ d"4326").funcall(Doc(sqlFunctionName))

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

  val ordinaryFunctionMap = (
    Seq[(Function[CT], OrdinaryFunctionSqlizer)](
      IsNull -> sqlizeIsNull,
      IsNotNull -> sqlizeIsNotNull,
      Not -> sqlizeUnaryOp("NOT"),

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
      Lt -> sqlizeBinaryOp("<"),
      Lte -> sqlizeBinaryOp("<="),
      Gt -> sqlizeBinaryOp(">"),
      Gte -> sqlizeBinaryOp(">="),
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

      FloatingTimeStampTruncYmd -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'day'")),
      FloatingTimeStampTruncYm -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'month'")),
      FloatingTimeStampTruncY -> sqlizeNormalOrdinaryFuncall("date_trunc", prefixArgs = Seq(d"'year'")),

      TextToPoint -> sqlizeGeomCast("st_pointfromtext"),
      TextToMultiPoint -> sqlizeGeomCast("st_mpointfromtext"),
      TextToLine -> sqlizeGeomCast("st_linefromtext"),
      TextToMultiLine -> sqlizeGeomCast("st_mlinefromtext"),
      TextToPolygon -> sqlizeGeomCast("st_polygonfromtext"),
      TextToMultiPolygon -> sqlizeGeomCast("st_mpolyfromtext"),

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

      ConcaveHull -> sqlizeMultiBuffered("st_concavehull"),
      ConvexHull -> sqlizeMultiBuffered("st_convexhull"),

      UrlToUrl -> sqlizeSubcol(SoQLUrl, "url"),
      UrlToDescription -> sqlizeSubcol(SoQLUrl, "description"),

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

  def numericize(sqlizer: AggregateFunctionSqlizer) = afs { (f, args, filter, ctx) =>
    val e = sqlizer(f, args, filter, ctx)
    assert(e.typ == SoQLNumber)
    ExprSql(e.compressed.sql +#+ d":: numeric", f)
  }

  val aggregateFunctionMap = (
    Seq[(Function[CT], AggregateFunctionSqlizer)](
      Max -> sqlizeNormalAggregateFuncall("max"),
      Min -> sqlizeNormalAggregateFuncall("max"),
      CountStar -> numericize(sqlizeCountStar _),
      Count -> numericize(sqlizeNormalAggregateFuncall("count")),
      CountDistinct -> numericize(sqlizeCountDistinct _),
      Sum -> sqlizeNormalAggregateFuncall("sum"),
      Avg -> sqlizeNormalAggregateFuncall("avg"),
      // Median
      // Median_disc
      // RegrIntercept
      // RegrSlope
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
      Ntile -> sqlizeLeadLag("ntile")
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
    assert(!e.function.isAggregate)
    assert(e.function.needsWindow)
    windowedFunctionMap(e.function.function.identity)(e, args, filter, partitionBy, orderBy, ctx)
  }
}
