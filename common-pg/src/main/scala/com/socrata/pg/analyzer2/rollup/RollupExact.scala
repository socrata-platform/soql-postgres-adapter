package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.pg.analyzer2.{SqlizerUniverse, RollupRewriter}

trait RollupExact[MT <: MetaTypes] extends SqlizerUniverse[MT] { this: HasLabelProvider =>
  private type IsoState = IsomorphismState.View[MT]

  // see if there's a rollup that can be used to answer _this_ select
  // (not any sub-parts of the select!).  This needs to produce a
  // statement with the same output schema (in terms of column labels
  // and types) as the given select.
  protected def rollupSelectExact(select: Select, candidate: Select, candidateName: types.ScopedResourceName[MT], candidateTableInfo: TableDescription.Dataset[MT]): Option[Statement] = {
    if(select.hint(SelectHint.NoRollup)) {
      return None
    }

    val isoState = select.from.isomorphicTo(candidate.from).getOrElse {
      return None
    }

    val rewriteInTerms = new RewriteInTerms(candidate.selectList, candidateName, candidateTableInfo)

    // we're both FROM the same thing, so now we can decide if this
    // Select can be expressed in terms of the output of that
    // candidate Select.

    // Cases:
    //   * neither select is aggregated
    //      * all expressions in select list, ORDER BY must be                 ✓
    //        expressible in terms of the output columns of candidate
    //      * WHERE must be isomorphic if the candidate has one,               ✓
    //        otherwise it must be expressible in terms of the output
    //        columns of candidate
    //      * WHERE and ORDER BY must be isomorphic if the candidate           ✓ (kinda)
    //        is windowed and the windowed expressions are used in
    //        this select.
    //      * SEARCH must not exist on either select                           ✓
    //      * DISTINCT or DISTINCT ON: interaction with window functions?      XXX
    //      * If LIMIT and/or OFFSET is specified, either the candidate        ✓
    //        must not specify or ORDER BY must be isomorphic and this
    //        function's LIMIT/OFFSET must specify a window completely
    //        contained within the candidate's LIMIT/OFFSET
    //   * this is aggregated but candidate is not
    //      * all expressions in the select list, GROUP BY, HAVING,            ✓
    //        and ORDER BY must be expressible in terms of the output
    //        columns of the candidate
    //      * WHERE must be isomorphic if the candidate has one,               ✓
    //        otherwise it must be expressible in terms of the output
    //        columns of candidate
    //      * SEARCH must not exist on either select                           ✓
    //      * Neither LIMIT nor OFFSET may exist on the candidate              ✓
    //   * candidate is aggregated but this is not
    //      * reject, candidate cannot be used                                 ✓
    //   * both are aggregated
    //      * the expressions in the select's GROUP BY must be a               ✓
    //        subset of the candidate's
    //      * WHERE must be isomorphic                                         ✓
    //      * all expressions in select list and ORDER BY must be
    //        expressible in terms of the output columns of candidate,
    //        under monoidal combination if the grouping is not the
    //        same
    //      * HAVING must be isomorphic and the GROUP BYs must be the
    //        same if the candidate has a HAVING, otherwise it must be
    //        expressible in terms of the output columns of candidate,
    //        under monoidal combination if the grouping is not the
    //        same
    //      * SEARCH must not exist on either select                           ✓
    //      * Neither LIMIT nor OFFSET may exist on the candidate              ✓

    (select.isAggregated, candidate.isAggregated) match {
      case (false, false) =>
        rewriteUnaggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (true, false) =>
        rewriteAggregatedOnUnaggregated(select, candidate, rewriteInTerms)
      case (false, true) =>
        trace("Bailing because cannot rewrite an un-aggregated query in terms of an aggregated one")
        None
      case (true, true) =>
        rewriteAggregatedOnAggregated(select, candidate, rewriteInTerms)
    }
  }

  private def rewriteUnaggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    trace("attempting to rewrite an unaggregated query in terms of an unaggregated candidate")

    assert(!select.isAggregated)
    assert(!candidate.isAggregated)
    assert(select.groupBy.isEmpty)
    assert(select.having.isEmpty)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    if(select.search.isDefined || candidate.search.isDefined) {
      trace("Bailing because SEARCH makes rollups bad")
      return None
    }

    val newWhere: Option[Expr] =
      candidate.where match {
        case Some(cWhere) =>
          select.where match {
            case Some(sWhere) =>
              if(!sWhere.isIsomorphic(cWhere, rewriteInTerms.isoState)) {
                trace("Bailing because WHERE is not isomorphic")
                return None
              }
              None // we'll just be using the candidate's WHERE
            case None =>
              trace("Bailing because candidate has a WHERE and we don't")
              return None
          }
        case None =>
          // Candidate has no WHERE but we do
          if(select.isWindowed) {
            trace("Bailing we have a WHERE and the candidate doesn't but we're windowed")
            return None
          }
          select.where match {
            case Some(sWhere) =>
              Some(
                rewriteInTerms(sWhere).getOrElse {
                  trace("Bailing because failed to rewrite WHERE")
                  return None
                })
            case None =>
              None
          }
      }

    lazy val orderByIsIsomorphic: Boolean =
      select.orderBy.length == candidate.orderBy.length &&
        (select.orderBy, candidate.orderBy).zipped.forall { (a, b) => a.isIsomorphic(b, rewriteInTerms.isoState) }

    if(candidate.isWindowed && !orderByIsIsomorphic) {
      // This isn't quite right!  We only need to bail here if one of
      // the candidate's windowed expressions is actually used in the
      // query.
      trace("Bailing because windowed but ORDER BYs are not isomorphic")
      return None
    }

    val newOrderBy: Seq[OrderBy] = select.orderBy.mapFallibly(rewriteInTerms(_)).getOrElse {
      trace("Bailing because couldn't rewrite ORDER BY in terms of candidate output columns")
      return None
    }

    val newSelectList: OrderedMap[AutoColumnLabel, NamedExpr] =
      select.selectList.iterator.mapFallibly(rewriteInTerms(_))

    val (newLimit, newOffset) =
      (candidate.limit, candidate.offset) match {
        case (None, None) =>
          (select.limit, select.offset)

        case (maybeLim, maybeOff) =>
          if(!orderByIsIsomorphic) {
            trace("Bailing because candidate has limit/offset but ORDER BYs are not isomorphic")
            return None
          }

          val sOffset = select.offset.getOrElse(BigInt(0))
          val cOffset = maybeOff.getOrElse(BigInt(0))

          if(sOffset < cOffset) {
            trace("Bailing because candidate has limit/offset but the query's window precedes it")
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
              trace("Bailing because candidate has a limit but the query does not")
              return None
            case (sLimit@Some(_), None) =>
              (sLimit, Some(newOffset))
            case (Some(sLimit), Some(cLimit)) =>
              if(newOffset + sLimit > cLimit) {
                trace("Bailing because candiate has a limit and the query's extends beyond its end")
                return None
              }

              (Some(sLimit), Some(newOffset))
          }
      }

    Some(
      Select(
        distinctiveness = ???,
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

  private def rewriteAggregatedOnUnaggregated(select: Select, candidate: Select, rewriteInTerms: RewriteInTerms): Option[Select] = {
    trace("attempting to rewrite an aggregated query in terms of an unaggregated candidate")

    assert(select.isAggregated)
    assert(!candidate.isAggregated)
    assert(candidate.groupBy.isEmpty)
    assert(candidate.having.isEmpty)

    if(select.search.isDefined || candidate.search.isDefined) {
      trace("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      trace("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    val newWhere =
      candidate.where match {
        case Some(cWhere) =>
          select.where match {
            case Some(sWhere) =>
              if(!sWhere.isIsomorphic(cWhere, rewriteInTerms.isoState)) {
                trace("Bailing because WHERE clauses where not isomorphic")
                return None
              }

              // We'll just be using the rollup's WHERE, so no need
              // for a where in the rewritten query.
              None
            case None =>
              trace("Bailing because candidate had a WHERE and the query does not")
              return None
          }
        case None =>
          select.where match {
            case Some(sWhere) =>
              Some(
                rewriteInTerms(sWhere).getOrElse {
                  trace("Bailing because unable to rewrite the WHERE in terms of the candidate's output columns")
                  return None
                }
              )
            case None =>
              None
          }
      }

    val newGroupBy = select.groupBy.mapFallibly(rewriteInTerms(_)).getOrElse {
      trace("Bailing because unable to rewrite the GROUP BY in terms of the candidate's output columns")
      return None
    }
    val newHaving = select.having.mapFallibly(rewriteInTerms(_)).getOrElse {
      trace("Bailing because unable to rewrite the HAVING in terms of the candidate's output columns")
      return None
    }
    val newOrderBy = select.orderBy.mapFallibly(rewriteInTerms(_)).getOrElse {
      trace("Bailing because unable to rewrite the ORDER BY in terms of the candidate's output columns")
      return None
    }
    val newSelectList = OrderedMap() ++ select.selectList.iterator.mapFallibly(rewriteInTerms(_)).getOrElse {
      trace("Bailing because unable to rewrite the select list in terms of the candidate's output columns")
      return None
    }

    Some(
      Select(
        distinctiveness = ???,
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
    trace("attempting to rewrite an aggregated query in terms of an aggregated candidate")

    assert(select.isAggregated)
    assert(candidate.isAggregated)

    if(select.search.isDefined || candidate.search.isDefined) {
      trace("Bailing because SEARCH makes rollups bad")
      return None
    }

    if(candidate.limit.isDefined || candidate.offset.isDefined) {
      trace("Bailing because the candidate has a LIMIT/OFFSET")
      return None
    }

    (select.where, candidate.where) match {
      case (None, None) =>
        // ok
      case (Some(sWhere), Some(cWhere)) =>
        if(!sWhere.isIsomorphic(cWhere, rewriteInTerms.isoState)) {
          trace("Bailing because WHERE was not isomorphic")
          return None
        }
      case (Some(_), None) =>
        trace("Bailing because the query had a WHERE and the candidate didn't")
        return None
      case (None, Some(_)) =>
        trace("Bailing because the candidate had a WHERE and the query didn't")
        return None
    }

    val selectGroupAssociations: Map[Expr, (Expr, Boolean)] =
      select.groupBy.mapFallibly { sGroupBy =>
        candidate.groupBy.find { cGroupBy =>
          sGroupBy.isIsomorphic(cGroupBy, rewriteInTerms.isoState)
        }.map((_, true)).orElse(
          candidate.groupBy.find { cGroupBy =>
            funcallSubset(sGroupBy, cGroupBy, rewriteInTerms.isoState)
          }.map((_, false))
        ).map(sGroupBy -> _)
      }.getOrElse {
        trace("Bailing because the query's GROUP BY was not a subset of the candidate's")
        return None
      }.toMap

    val candidateGroupByIsSubsetOfSelect = {
      val reversedIsoState = rewriteInTerms.isoState.reverse
      candidate.groupBy.forall { cGroupBy =>
        select.groupBy.exists { sGroupBy =>
          cGroupBy.isIsomorphic(sGroupBy, reversedIsoState) || funcallSubset(cGroupBy, sGroupBy, reversedIsoState)
        }
      }
    }

    if(candidateGroupByIsSubsetOfSelect) {
      // we're actually group by exactly the same set of things as the
      // candidate, so this is really a lot more like "unaggregated on
      // unaggregated".  In particular, we _don't_ need to combine
      // groups at all.

    } else {
      // Ok, when we do our rewrites we'll need to merge things using
      // selectGroupAssociations...

      Some(
        Select(
          distinctiveness = ???,
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
  }

  protected def funcallSubset(queryExpr: Expr, candidateExpr: Expr, under: IsoState): Boolean

  private class RewriteInTerms(
    val isoState: IsoState,
    candidateSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    candidateName: types.ScopedResourceName[MT],
    candidateTableInfo: TableDescription.Dataset[MT]
  ) {
    val newFrom = FromTable(
      candidateTableInfo.name,
      RollupRewriter.MAGIC_ROLLUP_CANONICAL_NAME,
      candidateName,
      None,
      labelProvider.tableLabel(),
      OrderedMap() ++ candidateTableInfo.columns.iterator.map { case (dtn, dci) =>
        dtn -> NameEntry(dci.name, dci.typ)
      },
      candidateTableInfo.primaryKeys
    )

    def apply(sExpr: Expr): Option[Expr] =
      ???

    def apply(sOrderBy: OrderBy): Option[OrderBy] =
      apply(sOrderBy.expr).map { newExpr => sOrderBy.copy(expr = newExpr) }

    def apply(sNamedExpr: NamedExpr): Option[NamedExpr] =
      apply(sNamedExpr.expr).map { newExpr => sNamedExpr.copy(expr = newExpr) }

    def apply(sSelectListEntry: (AutoColumnLabel, NamedExpr)): Option[(AutoColumnLabel, NamedExpr)] = {
      val (sLabel, sNamedExpr) = sSelectListEntry
      apply(sNamedExpr).map { newExpr => sLabel -> newExpr }
    }
  }

  private def trace(msg: String, args: Any*): Unit = {
    println(msg.format(args))
  }
}
