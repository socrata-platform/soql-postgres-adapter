package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

object FuncallSqlizer {
  case class DynamicContext[MT <: MetaTypes](
    repFor: Rep.Provider[MT],
    systemContext: Map[String, String],
    gensymProvider: GensymProvider
  ) {
    // This is kinda icky, but it lets us only set system context
    // settings if absolutely necessary, which in practice it should
    // never be.
    var nonliteralSystemContextLookupFound = false
  }
}

abstract class FuncallSqlizer[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  // Convention: if an expr might need parentheses for precedence
  // reason, it's the _caller's_ responsibility to add those
  // parentheses.

  // The intent here is that the relevant FuncallSqlizer be
  // instantiated _once_ and reused over the whole program (because a
  // FuncallSqlizer will likely contain a bunch of big maps, only a
  // portion of which will be used by any particular query); the
  // dynamic context is that part of funcall-sqlization that
  // potentially varies from call to call (e.g., building Reps
  // [because context providers] or the system context variables)
  type DynamicContext = FuncallSqlizer.DynamicContext[MT]

  type OrdinaryFunctionSqlizer = (FunctionCall, Seq[ExprSql], DynamicContext) => ExprSql
  protected def ofs(f: OrdinaryFunctionSqlizer) = f
  def sqlizeOrdinaryFunction(
    e: FunctionCall,
    args: Seq[ExprSql],
    ctx: DynamicContext
  ): ExprSql

  type AggregateFunctionSqlizer = (AggregateFunctionCall, Seq[ExprSql], Option[ExprSql], DynamicContext) => ExprSql
  def sqlizeAggregateFunction(
    e: AggregateFunctionCall,
    args: Seq[ExprSql],
    filter: Option[ExprSql],
    ctx: DynamicContext
  ): ExprSql
  protected def afs(f: AggregateFunctionSqlizer) = f

  type WindowedFunctionSqlizer = (WindowedFunctionCall, Seq[ExprSql], Option[ExprSql], Seq[ExprSql], Seq[OrderBySql], DynamicContext) => ExprSql
  def sqlizeWindowedFunction(
    e: WindowedFunctionCall,
    args: Seq[ExprSql],
    filter: Option[ExprSql],
    partitionBy: Seq[ExprSql],
    orderBy: Seq[OrderBySql],
    ctx: DynamicContext
  ): ExprSql
  protected def wfs(f: WindowedFunctionSqlizer) = f

  // These helpers don't care about the specific functions involved,
  // they're purely syntactic.

  protected def sqlizeUnaryOp(operator: String) = ofs { (e, args, ctx) =>
    assert(args.length == 1)
    assert(args.map(_.typ) == e.function.allParameters)

    val cmp = args.head.compressed

    ExprSql(Doc(operator) ++ cmp.sql.parenthesized, e)
  }

  protected def sqlizeBinaryOp(operator: String) = ofs { (e, args, ctx) =>
    assert(args.length == 2)
    assert(args.map(_.typ) == e.function.allParameters)

    val lhs = args(0).compressed.sql
    val rhs = args(1).compressed.sql

    ExprSql(lhs.parenthesized +#+ Doc(operator) +#+ rhs.parenthesized, e)
  }

  protected def sqlizeNormalOrdinaryFuncall(sqlFunctionName: String, prefixArgs: Seq[Doc] = Nil, suffixArgs: Seq[Doc] = Nil) = ofs { (e, args, ctx) =>
    assert(args.length >= e.function.minArity)
    assert(e.function.allParameters.startsWith(args.map(_.typ)))

    val sql = (prefixArgs ++ args.map(_.compressed.sql) ++ suffixArgs).funcall(Doc(sqlFunctionName))

    ExprSql(sql.group, e)
  }

  protected def sqlizeSubcol(typ: CT, field: String) = {
    ofs { (e, args, ctx) =>
      val rep = ctx.repFor(typ)
      assert(rep.expandedColumnCount != 1)

      val subcolInfo = rep.subcolInfo(field)

      assert(e.function.minArity == 1 && !e.function.isVariadic)
      assert(e.function.parameters.head == typ)
      assert(e.function.result == subcolInfo.soqlType)
      assert(args.length == 1)
      val arg = args.head
      assert(arg.typ == typ)

      arg match {
        case cmp: ExprSql.Compressed[MT] =>
          ExprSql(
            // This is kinda icky and fragile, but it'll work in
            // practice for the subcolumn types (text and number) that
            // we have.
            d"((" ++ cmp.sql ++ d")->>" ++ Doc(subcolInfo.index) ++ d") ::" +#+ Doc(subcolInfo.sqlType),
            e
          )
        case exp: ExprSql.Expanded[MT] =>
          ExprSql(
            exp.sqls(subcolInfo.index),
            e
          )
      }
    }
  }

  protected def sqlizeIdentity(e: FunctionCall, args: Seq[ExprSql], ctx: DynamicContext): ExprSql = {
    assert(e.function.minArity == 1 && !e.function.isVariadic)
    assert(args.lengthCompare(1) == 0)
    args(0)
  }

  // These helpers don't care about the particulars of the types in
  // any way that a Rep cannot answer, but they _are_ syntactic soql
  // features so will be shared among all sqlizers

  protected def sqlizeIsNull(e: FunctionCall, args: Seq[ExprSql], ctx: DynamicContext): ExprSql = {
    assert(args.length == 1)
    assert(args.map(_.typ) == e.function.allParameters)
    args.head match {
      case cmp: ExprSql.Compressed[MT] =>
        ExprSql(cmp.sql.parenthesized +#+ d"IS NULL", e)
      case exp: ExprSql.Expanded[MT] =>
        val result = exp.sqls.map { sql => sql.parenthesized +#+ d"IS NULL" }.concatWith { (l, r) => l ++ Doc.lineSep ++ d"AND" +#+ r }.nest(2).group
        ExprSql(result, e)
    }
  }

  protected def sqlizeIsNotNull(e: FunctionCall, args: Seq[ExprSql], ctx: DynamicContext): ExprSql = {
    assert(args.length == 1)
    assert(args.map(_.typ) == e.function.allParameters)
    args.head match {
      case cmp: ExprSql.Compressed[MT] =>
        ExprSql(cmp.sql.parenthesized +#+ d"IS NOT NULL", e)
      case exp: ExprSql.Expanded[MT] =>
        val result = exp.sqls.map { sql => sql.parenthesized +#+ d"IS NOT NULL" }.concatWith { (l, r) => l ++ Doc.lineSep ++ d"OR" +#+ r }.nest(2).group
        ExprSql(result, e)
    }
  }

  protected def sqlizeEq(e: FunctionCall, args: Seq[ExprSql], ctx: DynamicContext): ExprSql = {
    assert(args.length == 2)
    assert(args(0).typ == args(1).typ)
    assert(args.map(_.typ) == e.function.allParameters)

    args match {
      case Seq(a: ExprSql.Expanded[MT], b: ExprSql.Expanded[MT]) if ctx.repFor(a.typ).isProvenanced =>
        // Provenanced types are special in that they are atomic types
        // + a string which is not dependant on anything
        // metatype-specific, and are null if and only if their
        // "value" component is null.
        val Seq(aProv, aValue) = a.sqls
        val Seq(bProv, bValue) = b.sqls
        ExprSql(
          (d"(" ++ aProv.parenthesized +#+ d"IS NOT DISTINCT FROM" +#+ bProv.parenthesized ++ d") AND (" ++ aValue.parenthesized +#+ d"=" +#+ bValue.parenthesized ++ d")").group,
          e
        )
      case _ =>
        sqlizeBinaryOp("=")(e, args, ctx)
    }
  }

  protected def sqlizeNeq(e: FunctionCall, args: Seq[ExprSql], ctx: DynamicContext): ExprSql = {
    assert(args.length == 2)
    assert(args(0).typ == args(1).typ)
    assert(args.map(_.typ) == e.function.allParameters)

    args match {
      case Seq(a: ExprSql.Expanded[MT], b: ExprSql.Expanded[MT]) if ctx.repFor(a.typ).isProvenanced =>
        // Provenanced types are special in that they are atomic types
        // + a string which is not dependant on anything
        // metatype-specific, and are null if and only if their
        // "value" component is null.
        val Seq(aProv, aValue) = a.sqls
        val Seq(bProv, bValue) = b.sqls
        ExprSql(
          (d"(" ++ aProv.parenthesized +#+ d"IS DISTINCT FROM" +#+ bProv.parenthesized ++ d") OR (" ++ aValue.parenthesized +#+ d"=" +#+ bValue.parenthesized ++ d")").group,
          e
        )
      case _ =>
        sqlizeBinaryOp("<>")(e, args, ctx)
    }
  }

  protected def sqlizeInlike(operator: String) = ofs { (e, args, ctx) =>
    assert(e.function.minArity >= 1 && e.function.isVariadic)
    assert(args.length >= e.function.minArity)
    assert(e.function.allParameters.startsWith(args.map(_.typ)))

    val doc =
      args.head.compressed.sql.parenthesized +#+ Doc(operator) +#+ args.tail.map(_.compressed.sql).parenthesized

    ExprSql(doc.group, e)
  }

  // Aggregate functions

  protected def sqlizeFilter(filter: Option[ExprSql]) =
    filter.map { expr =>
      (d" FILTER (WHERE" ++ Doc.lineSep ++ expr.compressed.sql).nest(2) ++ Doc.lineCat ++ d")"
    }.getOrElse { Doc.empty }.group

  protected def sqlizeNormalAggregateFuncall(sqlFunctionName: String) = afs { (e, args, filter, ctx) =>
    assert(args.length >= e.function.minArity)
    assert(e.function.allParameters.startsWith(args.map(_.typ)))

    val sql = (
      Doc(sqlFunctionName) ++
        d"(" ++
        (if(e.distinct) d"DISTINCT" ++ Doc.lineSep else Doc.empty) ++
        args.map(_.compressed.sql).commaSep
    ).nest(2) ++
      Doc.lineCat ++
      d")" ++
      sqlizeFilter(filter)

    ExprSql(sql.group, e)
  }

  // aggregate functions with syntactic support

  protected def sqlizeCountStar(e: AggregateFunctionCall, args: Seq[ExprSql], filter: Option[ExprSql], ctx: DynamicContext) = {
    assert(args.length >= e.function.minArity)
    assert(e.function.allParameters.startsWith(args.map(_.typ)))

    // count(*) is slightly special in that it has no `distinct` clause
    assert(!e.distinct)

    val sql = d"count(*)" ++ sqlizeFilter(filter)

    ExprSql(sql, e)
  }

  // Window functions

  protected def sqlizeFrameContext(context: FrameContext) =
    context match {
      case FrameContext.Range => d"RANGE"
      case FrameContext.Rows => d"ROWS"
      case FrameContext.Groups => d"GROUPS"
    }

  protected def sqlizeFrameBound(start: FrameBound, end: Option[FrameBound]) = {
    def doc(bound: FrameBound) =
      bound match {
        case FrameBound.UnboundedPreceding => d"UNBOUNDED PRECEDING"
        case FrameBound.Preceding(n) => d"$n PRECEDING"
        case FrameBound.CurrentRow => d"CURRENT ROW"
        case FrameBound.Following(n) => d"$n FOLLOWING"
        case FrameBound.UnboundedFollowing => d"UNBOUNDED FOLLOWING"
      }

    end match {
      case None =>
        doc(start)
      case Some(end) =>
        (d"BETWEEN" ++ Doc.lineSep ++ doc(start) ++ Doc.lineSep ++ d"AND" +#+ doc(end)).nest(2).group
    }
  }

  protected def sqlizeFrameExclusion(excl: FrameExclusion) =
    d"EXCLUDE" ++ Doc.lineSep ++
      (excl match {
        case FrameExclusion.CurrentRow => d"CURRENT ROW"
        case FrameExclusion.Group => d"GROUP"
        case FrameExclusion.Ties => d"TIES"
        case FrameExclusion.NoOthers => d"NO OTHERS"
      })

  protected def sqlizeWindow(partitionBy: Seq[ExprSql], orderBy: Seq[OrderBySql], frame: Option[Frame]): Doc = {
    def partitionByDoc =
      (d"PARTITION BY" ++ Doc.lineSep ++ partitionBy.flatMap(_.sqls).commaSep).nest(2).group

    def orderByDoc =
      (d"ORDER BY" ++ Doc.lineSep ++ orderBy.flatMap(_.sqls).commaSep).nest(2).group

    def frameDoc(f: Frame) = {
      val Frame(context, start, end, exclusion) = f
        (sqlizeFrameContext(context) ++
           Doc.lineSep ++
           sqlizeFrameBound(start, end) ++
           exclusion.map(sqlizeFrameExclusion).map(Doc.lineSep ++ _).getOrElse(Doc.empty)).group
    }

    val items =
      Seq(
        if(partitionBy.nonEmpty) Some(partitionByDoc) else None,
        if(orderBy.nonEmpty) Some(orderByDoc) else None,
        frame.map(frameDoc)
      ).flatten

    if(items.isEmpty) {
      d" OVER ()"
    } else {
      ((d" OVER (" ++ Doc.lineCat ++ items.vsep).nest(2) ++ Doc.lineCat ++ d")").group
    }
  }

  protected def sqlizeNormalWindowedFuncall(sqlFunctionName: String) = { (e: WindowedFunctionCall, args: Seq[ExprSql], filter: Option[ExprSql], partitionBy: Seq[ExprSql], orderBy: Seq[OrderBySql], ctx: DynamicContext) =>
    assert(args.length >= e.function.minArity)
    assert(e.function.allParameters.startsWith(args.map(_.typ)))

    val sql = args.map(_.compressed.sql).funcall(Doc(sqlFunctionName)) ++
      sqlizeFilter(filter) ++
      sqlizeWindow(partitionBy, orderBy, e.frame)

    ExprSql(sql.group, e)
  }
}
