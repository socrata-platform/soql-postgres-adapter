package com.socrata.pg.analyzer2

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import org.slf4j.LoggerFactory

import com.socrata.pg.store.PGSecondaryUniverse

trait RollupRewriter[MT <: MetaTypes] {
  private var foundRollups = false

  final def applyRollups(analysis: SoQLAnalysis[MT]): SoQLAnalysis[MT] =
    // Once we've applied a set of rollups, we don't apply any others.
    // Might want to revisit this, as it's possible different rollups
    // may apply to different parts of the query after different sets
    // of rewrite passes.
    if(foundRollups) {
      analysis
    } else {
      attemptRollups(analysis) match {
        case Some(newAnalysis) =>
          foundRollups = true
          newAnalysis
        case None =>
          analysis
      }
    }

  protected def attemptRollups(analysis: SoQLAnalysis[MT]): Option[SoQLAnalysis[MT]]
}

object RollupRewriter {
  // Ick.  Ok, so what we're doing here is this: If a rollup table
  // contains a provenanced column, then _even though it's a physical
  // column_ we need to treat it specially in two ways.  First, it's
  // _actually_ a serialized virtual column (i.e., a prov column and
  // the actual physical value) so the Rep needs to know to load two
  // columns.  Second, various things over in ExprSqlizer and
  // FuncallSqlizer do a special optimized thing if a provenanced
  // column is known to have a single source-table.  For now, we'll
  // just not do that optimized thing if it's sourced from a rollup
  // tables.
  val MAGIC_ROLLUP_CANONICAL_NAME = CanonicalName("magic rollup canonical name")

  class Noop[MT <: MetaTypes] extends RollupRewriter[MT] {
    protected def attemptRollups(analysis: SoQLAnalysis[MT]): Option[SoQLAnalysis[MT]] =
      None
  }

  def fromPostgres[MT <: MetaTypes](pgu: PGSecondaryUniverse[MT#ColumnType, MT#ColumnValue]) =
    new Noop[MT]
}

trait HasLabelProvider {
  protected val labelProvider: LabelProvider
}

trait RRExperiments[MT <: MetaTypes] extends SqlizerUniverse[MT] { this: HasLabelProvider =>
  protected implicit def dtnOrdering: Ordering[MT#DatabaseColumnNameImpl]

  // see if there's a rollup that can be used to answer _this_ select
  // (not any sub-parts of the select!).  This needs to produce a
  // statement with the same output schema (in terms of column labels
  // and types) as the given select.
  protected def rollupSelectExact(select: Select): Option[Statement]

  // See if there's a rollup that can be used to answer _this_
  // combined tables (not any sub-parts of the combined tables!).
  // This needs to produce a statement with the same output schema (in
  // terms of column labels and types) as the given select.
  protected def rollupCombinedExact(combined: CombinedTables): Option[Statement]

  final def rollup(stmt: Statement): Option[Statement] = {
    rollup(stmt, prefixesAllowed = true)
  }

  private def rollup(stmt: Statement, prefixesAllowed: Boolean): Option[Statement] = {
    stmt match {
      case select: Select =>
        rollupSelectExact(select).
          orElse(if(prefixesAllowed) rollupPrefixes(select) else None).
          orElse(rollupSubqueries(select))
      case v: Values =>
        None
      case combined@CombinedTables(op, left, right) =>
        rollupCombinedExact(combined).orElse {
          val newLeft = rollup(left)
          val newRight = rollup(right)
          if(newLeft.isEmpty && newRight.isEmpty) {
            None
          } else {
            Some(CombinedTables(op, newLeft.getOrElse(left), newRight.getOrElse(right)))
          }
        }
      case _ : CTE =>
        None // don't have these yet, just punting on them...
    }
  }

  // This is a little awkward because rolling up a join-prefix is the
  // one case where schemas pre-rewrite and post-rewrite can change.
  // In particular, say we've got
  //
  //  select blah from @a join @b on cond where whatever etc
  //
  // and we have a rollup which is defined as "select @a.*, @b.* from @a join @b on cond".
  // In this case we can rewrite this as
  //
  //  select blah from (select the, demanded, columns from that_rollup) as @c where whatever
  //
  // BUT: column references in the analysis before would have expected to
  // access (virtual) tables @a and @b, and now they all need to reference
  // virtual table @c.  So doing that rewrite is the reason for the columnMap
  // stuff, which maps the "old" labels to the "new" labels.
  //
  // The most annoying part is that I'm not actually sure this is a
  // useful thing to do :)
  private def rollupPrefixes(select: Select): Option[Statement] =
    rollupPrefixes(select.from, select).map { case (newFrom, columnMap) =>
      fixupColumnReferences(select.copy(from = newFrom), columnMap)
    }

  private def rollupPrefixes(from: From, container: Select): Option[(From, Map[Col, VirtCol])] =
    from match {
      case af: AtomicFrom =>
        None
      case join: Join =>
        rollupPrefix(join, demandedColumns(join, container)).orElse {
          // One thing I'm unhappy about here is that we're
          // recomputing the demandedColumns for each prefix, which
          // involves a walk over the "container" select.  It'd be
          // nice not to do that.
          rollupPrefixes(join.left, container).map { case (newLeft, columnMap) =>
            val newRight = rollupAtomicFrom(join.right).getOrElse(join.right)
            (join.copy(left = newLeft, right = newRight), columnMap)
          }
        }
    }

  final def rollupSubqueries(select: Select): Option[Select] = {
    val (rewroteSomething, newFrom) = select.from.reduceMap[Boolean,MT](
      { leftmost =>
        rollupAtomicFrom(leftmost) match {
          case None => (false, leftmost)
          case Some(rewritten) => (true, rewritten)
        }
      },
      { (rewroteSomething, joinType, lateral, left, right, on) =>
        rollupAtomicFrom(right) match {
          case None => (rewroteSomething, Join(joinType, lateral, left, right, on))
          case Some(rewritten) => (true, Join(joinType, lateral, left, rewritten, on))
        }
      }
    )

    if(rewroteSomething) Some(select.copy(from = newFrom))
    else None
  }

  private def rollupAtomicFrom(from: AtomicFrom): Option[AtomicFrom] = {
    from match {
      case FromStatement(stmt, label, resourceName, alias) =>
        rollup(stmt).map(FromStatement(_, label, resourceName, alias))
      case other =>
        None
    }
  }

  // see if there's a rollup that matches `select ${all the columns} from $from`...
  private def rollupPrefix(from: Join, demandedColumns: Map[Col, CT]): Option[(FromStatement, Map[Col, VirtCol])] = {
    val ord1 = Ordering[(Int, Int)]
    val ord2 = Ordering[(Int, MT#DatabaseColumnNameImpl)]
    val orderedDemandedColumns: Seq[(Col, CT)] = demandedColumns.toSeq.
      sortWith { case ((col1, _ct1), (col2, _ct2)) =>
        (col1, col2) match {
          case (VirtCol(AutoTableLabel(atl1), AutoColumnLabel(acl1)),
                VirtCol(AutoTableLabel(atl2), AutoColumnLabel(acl2))) =>
            ord1.compare((atl1, acl1), (atl2, acl2)) < 0
          case (PhysCol(AutoTableLabel(atl1), _, DatabaseColumnName(dtn1)),
                PhysCol(AutoTableLabel(atl2), _, DatabaseColumnName(dtn2))) =>
            ord2.compare((atl1, dtn1), (atl2, dtn2)) < 0
          case (_ : VirtCol, _ : PhysCol) => true
          case (_ : PhysCol, _ : VirtCol) => false
        }
      }

    val newTable = labelProvider.tableLabel()
    val columnMap: Map[Col, VirtCol] = orderedDemandedColumns.iterator.map { case (col, _typ) =>
      col -> VirtCol(newTable, labelProvider.columnLabel())
    }.toMap

    val computedSelectList = OrderedMap() ++ orderedDemandedColumns.iterator.zipWithIndex.map { case ((col, typ), idx) =>
      columnMap(col).column -> NamedExpr(col.at(typ, AtomicPositionInfo.None), ColumnName(s"column_$idx"), isSynthetic = true)
    }

    val target =
      Select[MT](
        Distinctiveness.Indistinct(),
        computedSelectList,
        from,
        where = None,
        groupBy = Nil,
        having = None,
        orderBy = Nil,
        limit = None,
        offset = None,
        search = None,
        hint = Set.empty
      )

    rollup(target, prefixesAllowed = false).map { rewritten =>
      (FromStatement(rewritten, newTable, None, None), columnMap)
    }
  }

  private sealed abstract class Col {
    def at(typ: CT, pos: AtomicPositionInfo): Column
  }
  private case class PhysCol(table: AutoTableLabel, canonicalName: CanonicalName, column: DatabaseColumnName) extends Col {
    def at(typ: CT, pos: AtomicPositionInfo) = PhysicalColumn(table, canonicalName, column, typ)(pos)
  }
  private case class VirtCol(table: AutoTableLabel, column: AutoColumnLabel) extends Col {
    def at(typ: CT, pos: AtomicPositionInfo) = VirtualColumn(table, column, typ)(pos)
  }

  private def demandedColumns(prefix: Join, sel: Select): Map[Col, CT] = {
    class ColumnsFromFrom(from: Set[AutoTableLabel]) {
      val acc = new scm.HashMap[Col, CT]

      def go(stmt: Statement): Unit = {
        stmt match {
          case CombinedTables(_op, left, right) =>
            go(left)
            go(right)
          case CTE(_defLbl, _defAlias, defQ, _matHint, useQ) =>
            go(defQ)
            go(useQ)
          case Values(_labels, values) =>
            for {
              row <- values
              expr <- row
            } {
              go(expr)
            }
          case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, _limit, _offset, _search, _hint) =>
            go(distinctiveness)
            for(namedExpr <- selectList.values) go(namedExpr)
            go(from) // sadly, because of lateral joins, we need to recurse here
            for(w <- where) go(w)
            for(gb <- groupBy) go(gb)
            for(h <- having) go(h)
            for(ob <- orderBy) go(ob)
        }
      }

      def go(expr: Expr): Unit =
        expr match {
          case VirtualColumn(table, col, typ) if from(table) =>
            acc += VirtCol(table, col) -> typ
          case PhysicalColumn(table, canonName, col, typ) if from(table) =>
            acc += PhysCol(table, canonName, col) -> typ
          case FunctionCall(_func, args) =>
            for(arg <- args) go(arg)
          case AggregateFunctionCall(_func, args, _distinct, filter) =>
            for(arg <- args) go(arg)
            for(expr <- filter) go(expr)
          case WindowedFunctionCall(_func, args, filter, partitionBy, orderBy, _frame) =>
            for(arg <- args) go(arg)
            for(expr <- filter) go(expr)
            for(expr <- partitionBy) go(expr)
            for(ob <- orderBy) go(ob)
          case _ =>
            ()
        }

      def go(ob: OrderBy): Unit = go(ob.expr)
      def go(ne: NamedExpr): Unit = go(ne.expr)
      def go(distinctiveness: Distinctiveness): Unit =
        distinctiveness match {
          case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() => ()
          case Distinctiveness.On(exprs) => for(e <- exprs) go(e)
        }

      def go(from: From): Unit =
        from.reduce[Unit](
          goAtomicFrom(_),
          { (_, join) =>
            goAtomicFrom(join.right)
            go(join.on)
          }
        )

      def goAtomicFrom(from: AtomicFrom): Unit =
        from match {
          case _ : FromSingleRow => ()
          case _ : FromTable => ()
          case fs: FromStatement => go(fs.statement)
        }
    }

    def fromTables(from: From): Set[AutoTableLabel] = {
      from.reduce[Set[AutoTableLabel]](
        { leftmost => Set(leftmost.label) },
        { (acc, join) => acc + join.right.label }
      )
    }

    val walk = new ColumnsFromFrom(fromTables(prefix))
    walk.go(sel)
    val r = walk.acc.toMap
    println("demanded cols: " + r)
    r
  }

  private def fixupColumnReferences(sel: Select, columnMap: Map[Col, VirtCol]): Statement = {
    def goStmt(stmt: Statement): Statement = {
      stmt match {
        case ct@CombinedTables(_op, left, right) =>
          ct.copy(left = goStmt(left), right = goStmt(right))
        case cte@CTE(_defLbl, _defAlias, defQ, _matHint, useQ) =>
          cte.copy(definitionQuery = goStmt(defQ), useQuery = goStmt(useQ))
        case v@Values(_labels, values) =>
          v.copy(values = values.map { row => row.map(goExpr(_)) })
        case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          Select(
            goDistinct(distinctiveness),
            OrderedMap() ++ selectList.iterator.map { case (label, ne) => label -> goNamedExpr(ne) },
            goFrom(from),
            where.map(goExpr(_)),
            groupBy.map(goExpr(_)),
            having.map(goExpr(_)),
            orderBy.map(goOrderBy(_)),
            limit,
            offset,
            search,
            hint
          )
      }
    }

    def goFrom(from: From): From =
      from.map[MT](
        goAtomicFrom(_),
        (joinType, lateral, left, right, on) => Join(joinType, lateral, left, goAtomicFrom(right), goExpr(on))
      )

    def goAtomicFrom(af: AtomicFrom): AtomicFrom =
      af match {
        case fs: FromStatement => fs.copy(statement = goStmt(fs.statement))
        case fsr: FromSingleRow => fsr
        case ft: FromTable => ft
      }

    def goDistinct(distinct: Distinctiveness): Distinctiveness = {
      distinct match {
        case Distinctiveness.On(exprs) =>
          Distinctiveness.On(exprs.map(goExpr(_)))
        case other@(Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct()) =>
          other
      }
    }

    def goExpr(expr: Expr): Expr =
      expr match {
        case vc@VirtualColumn(table, col, typ) =>
          columnMap.get(VirtCol(table, col)) match {
            case Some(VirtCol(newTable, newCol)) =>
              VirtualColumn(newTable, newCol, typ)(vc.position)
            case None =>
              vc
          }
        case pc@PhysicalColumn(table, canonName, col, typ) =>
          columnMap.get(PhysCol(table, canonName, col)) match {
            case Some(VirtCol(newTable, newCol)) =>
              VirtualColumn(newTable, newCol, typ)(pc.position)
            case None =>
              pc
          }
        case fc@FunctionCall(func, args) =>
          FunctionCall(func, args.map(goExpr(_)))(fc.position)
        case afc@AggregateFunctionCall(func, args, distinct, filter) =>
          AggregateFunctionCall(
            func,
            args.map(goExpr(_)),
            distinct,
            filter.map(goExpr(_))
          )(afc.position)
        case wfc@WindowedFunctionCall(func, args, filter, partitionBy, orderBy, frame) =>
          WindowedFunctionCall(
            func,
            args.map(goExpr(_)),
            filter.map(goExpr(_)),
            partitionBy.map(goExpr(_)),
            orderBy.map(goOrderBy(_)),
            frame
          )(wfc.position)
        case other =>
          other
      }

    def goOrderBy(ob: OrderBy): OrderBy =
      ob.copy(expr = goExpr(ob.expr))

    def goNamedExpr(ne: NamedExpr): NamedExpr =
      ne.copy(expr = goExpr(ne.expr))

    goStmt(sel)
  }
}

trait ExprIsomorphism[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def isIsomorphicTo(e1: Expr, e2: Expr): Boolean
  def isIsomorphicTo(e1: Option[Expr], e2: Option[Expr]): Boolean =
    (e1, e2) match {
      case (None, None) => true
      case (Some(a), Some(b)) => isIsomorphicTo(a, b)
      case _ => false
    }
}

trait IsProjection[MT <: MetaTypes] extends SqlizerUniverse[MT] { this: ExprIsomorphism[MT] =>
  def isProjection(expr: Expr, candidates: Seq[(Expr, Expr)]): Option[Expr] = {
    expr match {
      case lit: LiteralValue =>
        Some(lit)
      case nul: NullLiteral =>
        Some(nul)
      case other =>
        for((candidate, columnRef) <- candidates) {
          if(isIsomorphicTo(expr, candidate)) {
            return Some(columnRef)
          } else {
            expr match {
              case fc@FunctionCall(func, args) =>
                val break = new scala.util.control.Breaks
                break.breakable {
                  val rewrittenArgs = args.map { arg =>
                    isProjection(arg, candidates) match {
                      case Some(columnRef) =>
                        columnRef
                      case None =>
                        break.break()
                    }
                  }
                  return Some(FunctionCall(func, rewrittenArgs)(fc.position))
                }
              case _ =>
                // nope
            }
          }
        }
        None
    }
  }
}

trait SemigroupRewriter[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  // This is responsible for doing things that look like
  //   count(x) => coalesce(sum(count_x), 0)
  //   max(x) => max(max_x)
  def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr]
}

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
      ) if isStrictSubset(groupBy, candidate.groupBy) && isIsomorphic(from, candidate.from) && isIsomorphicTo(where, candidate.where) =>
        val rewriter = exprRewriter(candidate.selectList, candidateName, candidateTableInfo)
        for {
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

  private def isStrictSubset(queryGroupBy: Seq[Expr], candidateGroupBy: Seq[Expr]): Boolean = {
    isSubset(queryGroupBy, candidateGroupBy) && !isSubset(candidateGroupBy, queryGroupBy)
  }

  private def isSubset(queryGroupBy: Seq[Expr], candidateGroupBy: Seq[Expr]): Boolean = {
    queryGroupBy.forall { qgb =>
      candidateGroupBy.exists { cgb =>
        isIsomorphicTo(qgb, cgb) || funcallSubset(qgb, cgb)
      }
    }
  }

  protected def funcallSubset(queryExpr: Expr, candidateExpr: Expr): Boolean

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
              merger(PhysicalColumn[MT](newFrom.label, newFrom.canonicalName, columnLabelMap(selectedColumn), rollup_expr.typ)(e.position.asAtomic))
            }
          case Some((selectedColumn, NamedExpr(_ : AggregateFunctionCall, _name, _isSynthetic))) =>
            // can't rewrite, the aggregate has a "distinct" or "filter"
            None
          case Some((selectedColumn, NamedExpr(_ : WindowedFunctionCall, _name, _isSynthetic))) =>
            // Can't rewrite
            None
          case Some((selectedColumn, ne)) =>
            assert(ne.expr.typ == e.typ)
            Some(PhysicalColumn[MT](newFrom.label, newFrom.canonicalName, columnLabelMap(selectedColumn), e.typ)(e.position.asAtomic))
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
