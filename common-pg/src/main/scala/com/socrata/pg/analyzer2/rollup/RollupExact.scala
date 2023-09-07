package com.socrata.pg.analyzer2.rollup

import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.pg.analyzer2.{SqlizerUniverse, RollupRewriter}

object RollupExact {
  private val log = LoggerFactory.getLogger(classOf[RollupExact[_]])
}

class RollupExact[MT <: MetaTypes](
  semigroupRewriter: SemigroupRewriter[MT],
  functionSubset: FunctionSubset[MT],
  splitAnd: SplitAnd[MT]
) extends SqlizerUniverse[MT] {
  import RollupExact.log

  private type IsoState = IsomorphismState.View[MT]

  // see if there's a rollup that can be used to answer _this_ select
  // (not any sub-parts of the select!).  This needs to produce a
  // statement with the same output schema (in terms of column labels
  // and types) as the given select.
  def rollupSelectExact(select: Select, rollupInfo: RollupInfo[MT], labelProvider: LabelProvider): Option[Statement] = {
    if(select.hint(SelectHint.NoRollup)) {
      return None
    }

    val candidate = rollupInfo.statement match {
      case sel: Select =>
        sel
      case _ =>
        log.debug("Bailing because rollup is not a Select")
        return None
    }

    val isoState = select.from.isomorphicTo(candidate.from).getOrElse {
      log.debug("Bailing because the rollup's From is not the same as the query's From")
      return None
    }

    val rewriteInTerms = new RewriteInTerms(isoState, candidate.selectList, rollupInfo.from(labelProvider))

    (select.isAggregated, candidate.isAggregated) match {
      case (false, false) =>
        rewriteUnaggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (true, false) =>
        rewriteAggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (false, true) =>
        log.debug("Bailing because cannot rewrite an un-aggregated query in terms of an aggregated one")
        None
      case (true, true) =>
        rewriteAggregatedOnAggregated(select, candidate, rewriteInTerms)
    }
  }

  private def rewriteUnaggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // * the candidate must be indistinct, or its DISTINCT clause         ✓
    //   must match (requires WHERE and ORDER isomorphism, and
    //   if fully distinct, select list isomorphism)
    // * all expressions in select list, ORDER BY must be                 ✓
    //   expressible in terms of the output columns of candidate
    // * WHERE must be isomorphic if the candidate has one,               ✓
    //   otherwise it must be expressible in terms of the output
    //   columns of candidate
    // * WHERE and ORDER BY must be isomorphic if the candidate           ✓ (kinda)
    //   is windowed and the windowed expressions are used in
    //   this select.
    // * SEARCH must not exist on either select                           ✓
    // * If LIMIT and/or OFFSET is specified, either the candidate        ✓
    //   must not specify or ORDER BY must be isomorphic and this
    //   function's LIMIT/OFFSET must specify a window completely
    //   contained within the candidate's LIMIT/OFFSET
    log.debug("attempting to rewrite an unaggregated query in terms of an unaggregated candidate")

    assert(!select.isAggregated)
    assert(!candidate.isAggregated)
    assert(select.groupBy.isEmpty)
    assert(select.having.isEmpty)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    if(select.search.isDefined || candidate.search.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }

    lazy val whereIsIsomorphic: Boolean =
      select.where.isDefined == candidate.where.isDefined &&
        (select.where, candidate.where).zipped.forall { (a, b) => a.isIsomorphic(b, rewriteInTerms.isoState) }

    val newWhere: Option[Expr] =
      candidate.where match {
        case Some(cWhere) =>
          if(!whereIsIsomorphic) {
            // We might still be able to deal with this...
            select.where match {
              case Some(sWhere) =>
                combineAnd(sWhere, cWhere, rewriteInTerms).getOrElse {
                  log.debug("Bailing because couldn't express the query's WHERE in terms of the candidate's WHERE")
                  return None
                }
              case None =>
                log.debug("Bailing because the candidate had a WHERE and the query does not")
                return None
            }
          } else {
            // Our where is the same as the candidate's where, so we can
            // just piggy-back on it.
            None
          }
        case None =>
          // Candidate has no WHERE but we do
          if(select.isWindowed) {
            log.debug("Bailing we have a WHERE and the candidate doesn't but we're windowed")
            return None
          }
          candidate.where.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
            log.debug("Bailing because couldn't rewrite WHERE in terms of candidate output columns")
            return None
          }
      }

    lazy val orderByIsIsomorphic: Boolean =
      select.orderBy.length == candidate.orderBy.length &&
        (select.orderBy, candidate.orderBy).zipped.forall { (a, b) => a.isIsomorphic(b, rewriteInTerms.isoState) }

    if(candidate.isWindowed && !orderByIsIsomorphic) {
      // This isn't quite right!  We only need to bail here if one of
      // the candidate's windowed expressions is actually used in the
      // query.
      log.debug("Bailing because windowed but ORDER BYs are not isomorphic")
      return None
    }

    val newOrderBy: Seq[OrderBy] = select.orderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_)).getOrElse {
      log.debug("Bailing because couldn't rewrite ORDER BY in terms of candidate output columns")
      return None
    }

    (select.distinctiveness, candidate.distinctiveness) match {
      case (sDistinct, Distinctiveness.Indistinct()) =>
        // ok, we can work with this
      case (Distinctiveness.FullyDistinct(), Distinctiveness.FullyDistinct()) =>
        // we need to select the exact same columns
        if(!sameSelectList(select.selectList, candidate.selectList, rewriteInTerms.isoState)) {
          log.debug("Bailing because DISTINCT but different select lists")
        }

        if(!whereIsIsomorphic || !orderByIsIsomorphic) {
          log.debug("Bailing because DISTINCT but WHERE or ORDER BY are not isomorphic")
          return None
        }
      case (sDistinct@Distinctiveness.On(sOn), Distinctiveness.On(cOn)) =>
        if(sOn.length != cOn.length || (sOn, cOn).zipped.exists { (s, c) => !s.isIsomorphic(c, rewriteInTerms.isoState) }) {
          log.debug("Bailing because of DISTINCT ON mismtach")
          return None
        }
        if(!whereIsIsomorphic || !orderByIsIsomorphic) {
          log.debug("Bailing because DISTINCT ON but WHERE or ORDER BY are not isomorphic")
          return None
        }
        val newOn = sOn.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
          log.debug("Bailing because failed to rewrite DISTINCT ON")
          return None
        }
      case _ =>
        log.debug("Bailing because of a distinctiveness mismatch")
        return None
      }
    val newDistinctiveness = rewriteDistinctiveness(select.distinctiveness, rewriteInTerms).getOrElse {
      log.debug("Bailing because failed to rewrite distinctiveness")
      return None
    }

    val newSelectList: OrderedMap[AutoColumnLabel, NamedExpr] =
      OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_)).getOrElse {
        log.debug("Bailing because failed to rewrite the select list")
        return None
      }

    val (newLimit, newOffset) =
      (candidate.limit, candidate.offset) match {
        case (None, None) =>
          (select.limit, select.offset)

        case (maybeLim, maybeOff) =>
          if(!orderByIsIsomorphic) {
            log.debug("Bailing because candidate has limit/offset but ORDER BYs are not isomorphic")
            return None
          }

          val sOffset = select.offset.getOrElse(BigInt(0))
          val cOffset = maybeOff.getOrElse(BigInt(0))

          if(sOffset < cOffset) {
            log.debug("Bailing because candidate has limit/offset but the query's window precedes it")
            return None
          }

          val newOffset = sOffset - cOffset

          (select.limit, maybeLim) match {
            case (None, None) =>
              if(newOffset == 0) {
                (None, None)
              } else {
                (None, Some(newOffset))
              }
            case (None, Some(_)) =>
              log.debug("Bailing because candidate has a limit but the query does not")
              return None
            case (sLimit@Some(_), None) =>
              (sLimit, Some(newOffset))
            case (Some(sLimit), Some(cLimit)) =>
              if(newOffset + sLimit > cLimit) {
                log.debug("Bailing because candiate has a limit and the query's extends beyond its end")
                return None
              }

              (Some(sLimit), Some(newOffset))
          }
      }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = newWhere,
        groupBy = Nil,
        having = None,
        orderBy = newOrderBy,
        limit = newLimit,
        offset = newOffset,
        search = None,
        hint = select.hint
      )
    )
  }

  private def sameSelectList(s: OrderedMap[AutoColumnLabel, NamedExpr], c: OrderedMap[AutoColumnLabel, NamedExpr], isoState: IsoState): Boolean = {
    // we don't care about order or duplicates, but we do care that
    // all exprs in s are isomorphic to exprs in c, and vice-versa.
    val sExprs = s.values.map(_.expr).toVector
    val cExprs = c.values.map(_.expr).toVector

    sameExprsIgnoringOrder(sExprs, cExprs, isoState)
  }

  private def sameExprsIgnoringOrder(s: Seq[Expr], c: Seq[Expr], isoState: IsoState): Boolean = {
    def isSubset(as: Seq[Expr], bs: Seq[Expr], isoState: IsoState): Boolean = {
      as.forall { a =>
        bs.exists { b => a.isIsomorphic(b, isoState) }
      }
    }

    isSubset(s, c, isoState) && isSubset(c, s, isoState.reverse)
  }

  private def rewriteDistinctiveness(distinct: Distinctiveness, rewriteInTerms: RewriteInTerms, needsMerge: Boolean = false): Option[Distinctiveness] = {
    distinct match {
      case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() =>
        Some(distinct)
      case Distinctiveness.On(exprs) =>
        exprs.mapFallibly(rewriteInTerms.rewrite(_, needsMerge)).map(Distinctiveness.On(_))
    }
  }

  private def rewriteAggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // * all expressions in the select list, GROUP BY, HAVING,            ✓
    //   and ORDER BY must be expressible in terms of the output
    //   columns of the candidate
    // * WHERE must be isomorphic if the candidate has one,               ✓
    //   otherwise it must be expressible in terms of the output
    //   columns of candidate
    // * SEARCH must not exist on either select                           ✓
    // * Neither LIMIT nor OFFSET may exist on the candidate              ✓
    // * candidate must not have a DISTINCT or DISTINCT ON                ✓

    log.debug("attempting to rewrite an aggregated query in terms of an unaggregated candidate")

    assert(select.isAggregated)
    assert(!candidate.isAggregated)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    if(select.search.isDefined || candidate.search.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      log.debug("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    candidate.distinctiveness match {
      case Distinctiveness.Indistinct() =>
        // ok
      case Distinctiveness.FullyDistinct() | Distinctiveness.On(_) =>
        log.debug("Bailing because the candidate has a DISTINCT clause")
        return None
    }
    val newDistinctiveness = rewriteDistinctiveness(select.distinctiveness, rewriteInTerms).getOrElse {
      log.debug("Bailing because failed to rewrite the query's DISTINCT clause")
      return None
    }

    val newWhere: Option[Expr] =
      candidate.where match {
        case Some(cWhere) =>
          select.where match {
            case Some(sWhere) =>
              combineAnd(sWhere, cWhere, rewriteInTerms).getOrElse {
                log.debug("Bailing because couldn't express the query's WHERE in terms of the candidate's WHERE")
                return None
              }
            case None =>
              log.debug("Bailing because candidate had a WHERE and the query does not")
              return None
          }
        case None =>
          select.where.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
            log.debug("Bailing because unable to rewrite the WHERE in terms of the candidate's output columns")
            return None
          }
      }

    val newGroupBy = select.groupBy.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
      log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }
    val newHaving = select.having.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
      log.debug("Bailing because unable to rewrite the HAVING in terms of the candidate's output columns")
      return None
    }
    val newOrderBy = select.orderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_)).getOrElse {
      log.debug("Bailing because unable to rewrite the ORDER BY in terms of the candidate's output columns")
      return None
    }
    val newSelectList = OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_)).getOrElse {
      log.debug("Bailing because unable to rewrite the select list in terms of the candidate's output columns")
      return None
    }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = newWhere,
        groupBy = newGroupBy,
        having = newHaving,
        orderBy = newOrderBy,
        limit = select.limit,
        offset = select.offset,
        search = None,
        hint = select.hint
      )
    )
  }

  private def rewriteAggregatedOnAggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    // * the expressions in the select's GROUP BY must be a               ✓
    //   subset of the candidate's
    // * WHERE must be isomorphic                                         ✓
    // * all expressions in select list and ORDER BY must be              ✓
    //   expressible in terms of the output columns of candidate,
    //   under monoidal combination if the grouping is not the
    //   same
    // * HAVING must be isomorphic and the GROUP BYs must be the          ✓
    //   same if the candidate has a HAVING, otherwise it must be
    //   expressible in terms of the output columns of candidate,
    //   under monoidal combination if the grouping is not the
    //   same
    // * SEARCH must not exist on either select                           ✓
    // * Neither LIMIT nor OFFSET may exist on the candidate              ✓
    // * candidate must not have a DISTINCT or DISTINCT ON                ✓

    log.debug("attempting to rewrite an aggregated query in terms of an aggregated candidate")

    assert(select.isAggregated)
    assert(candidate.isAggregated)

    if(select.search.isDefined || candidate.search.isDefined) {
      log.debug("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      log.debug("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    (select.where, candidate.where) match {
      case (None, None) =>
        // ok
      case (Some(sWhere), Some(cWhere)) =>
        if(!sWhere.isIsomorphic(cWhere, rewriteInTerms.isoState)) {
          log.debug("Bailing because WHERE was not isomorphic")
          return None
        }
      case (Some(_), None) =>
        log.debug("Bailing because the query had a WHERE and the candidate didn't")
        return None
      case (None, Some(_)) =>
        log.debug("Bailing because the candidate had a WHERE and the query didn't")
        return None
    }

    val groupBySubset =
      select.groupBy.forall { sGroupBy =>
        candidate.groupBy.exists { cGroupBy =>
          sGroupBy.isIsomorphic(cGroupBy, rewriteInTerms.isoState)
        } || candidate.groupBy.exists { cGroupBy =>
          functionSubset.funcallSubset(sGroupBy, cGroupBy, rewriteInTerms.isoState).isDefined
        }
      }
    if(!groupBySubset) {
      log.debug("Bailing because the query's GROUP BY was not a subset of the candidate's")
      return None
    }

    val sameGroupBy = sameExprsIgnoringOrder(select.groupBy, candidate.groupBy, rewriteInTerms.isoState)

    val needsMerge = !sameGroupBy

    candidate.distinctiveness match {
      case Distinctiveness.Indistinct() =>
        // ok
      case Distinctiveness.FullyDistinct() | Distinctiveness.On(_) =>
        log.debug("Bailing because the candidate has a DISTINCT clause")
        return None
    }
    val newDistinctiveness = rewriteDistinctiveness(select.distinctiveness, rewriteInTerms, needsMerge).getOrElse {
      log.debug("Bailing because failed to rewrite the DISTINCT clause")
      return None
    }

    val newSelectList: OrderedMap[AutoColumnLabel, NamedExpr] =
      OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms.rewriteSelectListEntry(_, needsMerge)).getOrElse {
        log.debug("Bailing because failed to rewrite the select list")
        return None
      }

    val newGroupBy = select.groupBy.mapFallibly(rewriteInTerms.rewrite(_, needsMerge)).getOrElse {
      log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }

    val newHaving = (select.having, candidate.having) match {
      case (sHaving, None) =>
        sHaving.mapFallibly(rewriteInTerms.rewrite(_)).getOrElse {
          log.debug("Bailing because unable to rewrite the HAVING in terms of the candidate's output columns")
          return None
        }
      case (Some(sHaving), Some(cHaving)) =>
        val combined = combineAnd(sHaving, cHaving, rewriteInTerms).getOrElse {
          log.debug("Bailing because couldn't express the query's HAVING in terms of the candidate's HAVING")
          return None
        }

        if(!sameGroupBy) {
          log.debug("Bailing because both have a HAVING but the GROUP BY clauses are different")
          return None
        }

        // Our GROUP BYs and HAVINGs are the same so since we're just
        // going to be using the rollup's having.
        combined
      case (None, Some(_)) =>
        log.debug("Bailing because the candidate has a HAVING clause and the query does not")
        return None
    }

    val newOrderBy = select.orderBy.mapFallibly(rewriteInTerms.rewriteOrderBy(_, needsMerge)).getOrElse {
      log.debug("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }

    Some(
      Select(
        distinctiveness = newDistinctiveness,
        selectList = newSelectList,
        from = rewriteInTerms.newFrom,
        where = None,
        groupBy = newGroupBy,
        having = newHaving,
        orderBy = newOrderBy,
        limit = select.limit,
        offset = select.offset,
        search = None,
        hint = select.hint
      )
    )
  }

  private def combineAnd(sExpr: Expr, cExpr: Expr, rewriteInTerms: RewriteInTerms): Option[Option[Expr]] = {
    val sSplit = splitAnd.splitAnd(sExpr)
    val cSplit = splitAnd.splitAnd(cExpr)

    // ok, this is a little subtle.  We have two lists of AND clauses,
    // and we want to make sure that cSplit is a subseqence of sSplit,
    // to preserve short-circuiting.
    var remaining = cSplit.toList
    val result = Vector.newBuilder[Expr]
    for(sClause <- sSplit) {
      remaining match {
        case cClause :: cRemaining =>
          if(sClause.isIsomorphic(cClause, rewriteInTerms.isoState)) {
            remaining = cRemaining
          } else {
            result += sClause
          }
        case Nil =>
          result += sClause
      }
    }

    if(remaining.nonEmpty) {
      log.debug("Leftover candidate clauses after scan")
      return None
    }

    splitAnd.mergeAnd(result.result()).mapFallibly(rewriteInTerms.rewrite(_))
  }

  private class RewriteInTerms(
    val isoState: IsoState,
    candidateSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    val newFrom: FromTable
  ) {
    // Check that our candidate select list and table info are in
    // sync, and build a map from AutoColumnLabel to
    // DatabaseColumnName so when we're rewriting exprs we can fill in
    // with references to PhysicalColumns
    assert(candidateSelectList.size == newFrom.schema.length)
    private val columnLabelMap: Map[AutoColumnLabel, DatabaseColumnName] =
      (candidateSelectList.toStream, newFrom.columns).zipped.map { case ((label, namedExpr), (databaseColumnName, nameEntry)) =>
        assert(namedExpr.name == nameEntry.name && namedExpr.expr.typ == nameEntry.typ)

        label -> databaseColumnName
      }.toMap

    def rewrite(sExpr: Expr, needsMerge: Boolean = false): Option[Expr] =
      candidateSelectList.iterator.findMap { case (label, ne) =>
        if(sExpr.isIsomorphic(ne.expr, isoState)) {
          Some((label, ne, identity[Expr] _))
        } else {
          None
        }
      }.orElse(
        candidateSelectList.iterator.findMap { case (label, ne) =>
          functionSubset.funcallSubset(sExpr, ne.expr, isoState).map((label, ne, _))
        }
      ) match {
        case Some((selectedColumn, NamedExpr(rollupExpr@AggregateFunctionCall(func, args, false, None), _name, _isSynthetic), functionExtract)) if needsMerge =>
          assert(rollupExpr.typ == sExpr.typ)
          semigroupRewriter.mergeSemigroup(func).map { merger =>
            functionExtract(merger(PhysicalColumn[MT](newFrom.label, newFrom.tableName, newFrom.canonicalName, columnLabelMap(selectedColumn), rollupExpr.typ)(sExpr.position.asAtomic)))
          }
        case Some((selectedColumn, NamedExpr(_ : AggregateFunctionCall, _name, _isSynthetic), _)) if needsMerge =>
          log.debug("can't rewrite, the aggregate has a 'distinct' or 'filter'")
          None
        case Some((selectedColumn, NamedExpr(_ : WindowedFunctionCall, _name, _isSynthetic), _)) if needsMerge =>
          log.debug("can't rewrite, windowed function call")
          None
        case Some((selectedColumn, ne, functionExtract)) =>
          assert(ne.expr.typ == sExpr.typ)
          Some(functionExtract(PhysicalColumn[MT](newFrom.label, newFrom.tableName, newFrom.canonicalName, columnLabelMap(selectedColumn), sExpr.typ)(sExpr.position.asAtomic)))
        case None =>
          sExpr match {
            case lit: LiteralValue => Some(lit)
            case nul: NullLiteral => Some(nul)
            case slr: SelectListReference => throw new AssertionError("Rollups should never see a SelectListReference")
            case fc@FunctionCall(func, args) =>
              args.mapFallibly(rewrite(_, needsMerge)).map { newArgs =>
                FunctionCall(func, newArgs)(fc.position)
              }
            case afc@AggregateFunctionCall(func, args, distinct, filter) if !needsMerge =>
              for {
                newArgs <- args.mapFallibly(rewrite(_, needsMerge))
                newFilter <- filter.mapFallibly(rewrite(_, needsMerge))
              } yield {
                AggregateFunctionCall(func, newArgs, distinct, newFilter)(afc.position)
              }
            case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) if !needsMerge =>
              for {
                newArgs <- args.mapFallibly(rewrite(_, needsMerge))
                newFilter <- filter.mapFallibly(rewrite(_, needsMerge))
                newPartitionBy <- partitionBy.mapFallibly(rewrite(_, needsMerge))
                newOrderBy <- orderBy.mapFallibly(rewriteOrderBy(_, needsMerge))
              } yield {
                WindowedFunctionCall(func, args, newFilter, newPartitionBy, newOrderBy, frame)(wfc.position)
              }
            case other =>
              log.debug("Can't rewrite, didn't find an appropriate source column")
              None
          }
      }

    def rewriteOrderBy(sOrderBy: OrderBy, needsMerge: Boolean = false): Option[OrderBy] =
      rewrite(sOrderBy.expr, needsMerge).map { newExpr => sOrderBy.copy(expr = newExpr) }

    def rewriteNamedExpr(sNamedExpr: NamedExpr, needsMerge: Boolean = false): Option[NamedExpr] =
      rewrite(sNamedExpr.expr, needsMerge).map { newExpr => sNamedExpr.copy(expr = newExpr) }

    def rewriteSelectListEntry(sSelectListEntry: (AutoColumnLabel, NamedExpr), needsMerge: Boolean = false): Option[(AutoColumnLabel, NamedExpr)] = {
      val (sLabel, sNamedExpr) = sSelectListEntry
      rewriteNamedExpr(sNamedExpr, needsMerge).map { newExpr => sLabel -> newExpr }
    }
  }
}
