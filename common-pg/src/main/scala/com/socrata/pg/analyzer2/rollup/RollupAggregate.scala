package com.socrata.pg.analyzer2.rollup

import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.pg.analyzer2.{SqlizerUniverse, RollupRewriter}

// Cases!
//    Rollup is a grouped view.  In that case it can be used
//       * for exact-match rewrites
//       * for rollup rewrites
//
// Rollup-rewrites:
//   GIVEN
//     a rollup that looks like SELECT s1 FROM f WHERE w GROUP BY x, y, z...
//     a query that looks like SELECT s2 FROM f WHERE w GROUP BY a strict subset of x, y, z... HAVING h ORDER BY o OFFSET off LIMIT lim
//   WHERE
//     aggregate functions in s2 form a monoid over their types
//   THEN
//     the query can be rewritten into SELECT reagg(s2) FROM rollup GROUP BY (subset) HAVING h ORDER BY o OFFSET off LIMIT lim
//   NOTE THAT
//     s1 _MUST INCLUDE ALL THE GROUPING COLUMNS_ (so that e.g., a reference to "x" can be rewritten into a reference to s1's version of "x")
//     date_trunc_ymd(x) is a subset of x
//     date_trunc_ym(x) is a subset of x and date_trunc_ymd(x)
//     (etc)

trait RollupAggregate[MT <: MetaTypes] extends SqlizerUniverse[MT] { this: SemigroupRewriter[MT] with ExprIsomorphism[MT] with HasLabelProvider =>
  private type IsoState = IsomorphismState.View[MT]

  def rollupAggregate(query: Select, candidate: Select, candidateName: types.ScopedResourceName[MT], candidateTableInfo: TableDescription.Dataset[MT]): Option[Select] = {
    assert(!query.hint(SelectHint.NoRollup))
    assert(query.isAggregated)
    assert(candidate.isAggregated)

    query match {
      case Select(
        distinct@Distinctiveness.Indistinct(),
        selectList,
        from,
        where,
        groupBy,
        having,
        orderBy,
        offset,
        limit,
        None, // won't roll up if a SEARCH is involved
        hint
      ) =>
        for {
          isoState <- from.isomorphicTo(candidate.from)
          if isStrictSubset(groupBy, candidate.groupBy, isoState)
          if isIsomorphic(where, candidate.where, isoState)
          rewriter = exprRewriter(candidate.selectList, candidateName, candidateTableInfo)
          newSelectList <- rewriter.rewriteSelectList(selectList)
          newGroupBy <- mapMaybe(groupBy, rewriter.rewriteExpr)
          // "having" is None if there is no having, but we want
          // Some(None) in that case; having.map(rewriteExpr) is
          // Some(None) if if it exists can't be rewritten, but we
          // want None in that case.
          newHaving <- transposeOption(having.map(rewriter.rewriteExpr))
          newOrderBy <- mapMaybe(orderBy, rewriter.rewriteOrderBy)
        } yield {
          Select(
            distinct,
            newSelectList,
            rewriter.newFrom,
            None,
            newGroupBy,
            newHaving,
            newOrderBy,
            offset,
            limit,
            None,
            hint + SelectHint.NoRollup
          )
        }
      case _ =>
        // Query isn't the right shape, won't rewrite
        None
    }
  }

  private def isIsomorphic(a: Option[Expr], b: Option[Expr], under: IsoState): Boolean =
    (a, b) match {
      case (Some(a), Some(b)) => a.isIsomorphic(b, under)
      case (None, None) => true
      case _ => false
    }

  private def transposeOption[T](o: Option[Option[T]]): Option[Option[T]] =
    o match {
      case Some(None) => None
      case None => Some(None)
      case Some(Some(v)) => Some(Some(v))
    }

  private def mapMaybe[T, U](a: Seq[T], f: T => Option[U]): Option[Seq[U]] = {
    Some(a.map { t =>
      f(t).getOrElse {
        return None
      }
    })
  }

  private def isIsomorphic(a: From, b: From): Boolean = {
    val fakeA = Select(Distinctiveness.Indistinct(), OrderedMap(), a, None, Nil, None, Nil, None, None, None, Set.empty)
    val fakeB = Select(Distinctiveness.Indistinct(), OrderedMap(), b, None, Nil, None, Nil, None, None, None, Set.empty)
    fakeA.isIsomorphic(fakeB)
  }

  private def isStrictSubset(queryGroupBy: Seq[Expr], candidateGroupBy: Seq[Expr], under: IsoState): Boolean = {
    isSubset(queryGroupBy, candidateGroupBy, under) && !isSubset(candidateGroupBy, queryGroupBy, under.reverse)
  }

  private def isSubset(queryGroupBy: Seq[Expr], candidateGroupBy: Seq[Expr], under: IsoState): Boolean = {
    queryGroupBy.forall { qgb =>
      candidateGroupBy.exists { cgb =>
        qgb.isIsomorphic(cgb, under) || funcallSubset(qgb, cgb, under)
      }
    }
  }

  protected def funcallSubset(queryExpr: Expr, candidateExpr: Expr, under: IsoState): Boolean

  private def exprRewriter(
    candidateSelectList: OrderedMap[AutoColumnLabel, NamedExpr],
    rollupName: types.ScopedResourceName[MT],
    candidateTableInfo: TableDescription.Dataset[MT]
  ): ExprRewriter = {
    new ExprRewriter {
      // Check that our candidate select list and table info are in
      // sync, and build a map from AutoColumnLabel to
      // DatabaseColumnName so when we're rewriting exprs we can fill in
      // with references to PhysicalColumns
      assert(candidateSelectList.size == candidateTableInfo.columns.size)
      val columnLabelMap: Map[AutoColumnLabel, DatabaseColumnName] =
        candidateSelectList.iterator.map { case (label, namedExpr) =>
          val (databaseColumnName, _) =
            candidateTableInfo.columns.find { case (dcn, dci) =>
              dci.name == namedExpr.name
            }.getOrElse {
              throw new AssertionError("No corresponding column in rollup table for " + namedExpr.name)
            }
          label -> databaseColumnName
        }.toMap

      val newFrom = FromTable(
        candidateTableInfo.name,
        RollupRewriter.MAGIC_ROLLUP_CANONICAL_NAME,
        rollupName,
        None,
        labelProvider.tableLabel(),
        OrderedMap() ++ candidateTableInfo.columns.iterator.map { case (dtn, dci) =>
          dtn -> NameEntry(dci.name, dci.typ)
        },
        candidateTableInfo.primaryKeys
      )

      def rewriteExpr(e: Expr): Option[Expr] = {
        // ok so here we go rewriting.  Our cases are:
        //   * "e" is an aggregate which is isomorphic to one of the selected columns, which needs to be merged
        //   * "e" is isomorphic to one of the selected columns
        //   * "e" is a normal function whose arguments need to be rewritten
        //   * "e" is a literal

        candidateSelectList.iterator.find { case (_, ne) =>
          isIsomorphicTo(e, ne.expr)
        } match {
          case Some((selectedColumn, NamedExpr(rollup_expr@AggregateFunctionCall(func, args, false, None), _name, _isSynthetic))) =>
            assert(rollup_expr.typ == e.typ)
            mergeSemigroup(func).map { merger =>
              merger(PhysicalColumn[MT](newFrom.label, newFrom.tableName, newFrom.canonicalName, columnLabelMap(selectedColumn), rollup_expr.typ)(e.position.asAtomic))
            }
          case Some((selectedColumn, NamedExpr(_ : AggregateFunctionCall, _name, _isSynthetic))) =>
            // can't rewrite, the aggregate has a "distinct" or "filter"
            None
          case Some((selectedColumn, NamedExpr(_ : WindowedFunctionCall, _name, _isSynthetic))) =>
            // Can't rewrite
            None
          case Some((selectedColumn, ne)) =>
            assert(ne.expr.typ == e.typ)
            Some(PhysicalColumn[MT](newFrom.label, newFrom.tableName, newFrom.canonicalName, columnLabelMap(selectedColumn), e.typ)(e.position.asAtomic))
          case None =>
            e match {
              case lit: LiteralValue => Some(lit)
              case nul: NullLiteral => Some(nul)
              case fc@FunctionCall(func, args) =>
                mapMaybe(args, rewriteExpr).map { newArgs =>
                  FunctionCall(func, newArgs)(fc.position)
                }
              case other =>
                LoggerFactory.getLogger(classOf[ExprRewriter]).debug("Couldn't rewrite {}", other)
                None
            }
        }
      }
    }
  }

  private trait ExprRewriter {
    def newFrom: FromTable
    def rewriteExpr(e: Expr): Option[Expr]

    final def rewriteOrderBy(ob: OrderBy) = rewriteExpr(ob.expr).map { newExpr => ob.copy(expr = newExpr) }
    final def rewriteSelectList(sl: OrderedMap[AutoColumnLabel, NamedExpr]): Option[OrderedMap[AutoColumnLabel, NamedExpr]] = {
      mapMaybe[(AutoColumnLabel, NamedExpr), (AutoColumnLabel, NamedExpr)](sl.toSeq, { case (acl, ne) =>
        rewriteExpr(ne.expr).map { newExpr =>
          acl -> ne.copy(expr = newExpr)
        }
      }).map { newItems =>
        OrderedMap() ++ newItems
      }
    }
  }
}
