package com.socrata.pg.analyzer2.rollup

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName

import com.socrata.pg.analyzer2.SqlizerUniverse

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
}

class RollupRewriter[MT <: MetaTypes](
  labelProvider: LabelProvider,
  rollupExact: RollupExact[MT],
  rollups: Seq[RollupInfo[MT]]
)(implicit dtnOrdering: Ordering[MT#DatabaseColumnNameImpl]) extends SqlizerUniverse[MT] {
  // see if there are rollups that can be used to answer _this_ select
  // (not any sub-parts of the select!).  This needs to produce
  // statements with the same output schema (in terms of column labels
  // and types) as the given select.
  private def rollupSelectExact(select: Select): Seq[Statement] =
    rollups.flatMap(rollupExact(select, _, labelProvider))

  // See if there are rollup that can be used to answer _this_
  // combined tables (not any sub-parts of the combined tables!).
  // This needs to produce statements with the same output schema (in
  // terms of column labels and types) as the given select.
  private def rollupCombinedExact(combined: CombinedTables): Seq[Statement] = {
    rollups.flatMap {
      // This is stronger than it needs to be.  Ways it can be
      // weakened:
      //    the select lists don't need to be so sensitive to ordering
      //    if the op is UNION ALL we don't need exact match on the select-lists
      case ri if combined.isIsomorphic(ri.statement) =>

        val from = ri.from(labelProvider)

        Some(
          Select(
            Distinctiveness.Indistinct(),
            OrderedMap() ++ (combined.schema.toStream, from.columns.toStream).zipped.map {
              case ((sourceLabel, sourceEnt), (rollupCol, rollupEnt)) =>
                assert(sourceEnt.name == rollupEnt.name)
                assert(sourceEnt.typ == rollupEnt.typ)
                sourceLabel -> NamedExpr(
                  PhysicalColumn[MT](from.label, from.tableName, from.canonicalName, rollupCol, sourceEnt.typ)(AtomicPositionInfo.None),
                  sourceEnt.name,
                  sourceEnt.isSynthetic
                )
            },
            from,
            None,
            Nil,
            None,
            Nil,
            None,
            None,
            None,
            Set.empty
          )
        )
      case _ =>
        None
    }
  }

  // Attempt to roll up this statement; this will produce no results
  // if no rollups apply.
  final def rollup(stmt: Statement): Seq[Statement] = {
    rollup(stmt, prefixesAllowed = true)
  }

  // Attempt to roll up this statement; this will produce no results
  // if no rollups apply.
  private def rollup(stmt: Statement, prefixesAllowed: Boolean): Seq[Statement] = {
    stmt match {
      case select: Select =>
        if(select.hint(SelectHint.NoRollup)) {
          Nil
        } else {
          rollupSelectExact(select) ++
            (if(prefixesAllowed) rollupPrefixes(select) else None) ++
            rollupSubqueries(select)
        }
      case v: Values =>
        Nil
      case combined@CombinedTables(op, left, right) =>
        rollupCombinedExact(combined) ++ {
          val newLefts = rollup(left)
          val newRights = rollup(right)
          if(newLefts.isEmpty && newRights.isEmpty) {
            Nil
          } else {
            for {
              newLeft <- left +: newLefts
              newRight <- right +: newRights
              if (newLeft ne left) || (newRight ne right)
            } yield {
              CombinedTables(op, newLeft, newRight)
            }
          }
        }
      case _ : CTE =>
        Nil // don't have these yet, just punting on them...
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
  private def rollupPrefixes(select: Select): Seq[Statement] =
    rollupPrefixes(select.from, select).map { case (newFrom, columnMap) =>
      fixupColumnReferences(select.copy(from = newFrom), columnMap)
    }

  private def rollupPrefixes(from: From, container: Select): Seq[(From, Map[Col, VirtCol])] =
    from match {
      case af: AtomicFrom =>
        Nil
      case join: Join =>
        rollupPrefix(join, demandedColumns(join, container)) ++ {
          // One thing I'm unhappy about here is that we're
          // recomputing the demandedColumns for each prefix, which
          // involves a walk over the "container" select.  It'd be
          // nice not to do that.
          rollupPrefixes(join.left, container).flatMap { case (newLeft, columnMap) =>
            rollupAtomicFrom(join.right).map { newRight =>
              (join.copy(left = newLeft, right = newRight), columnMap)
            }
          }
        }
    }

  final def rollupSubqueries(select: Select): Seq[Select] = {
    val newFroms = select.from.reduce[Seq[From]](
      { leftmost => leftmost +: rollupAtomicFrom(leftmost) },
      { (acc, join) =>
        val Join(joinType, lateral, left, right, on) = join
        for {
          newLeft <- acc
          newRight <- right +: rollupAtomicFrom(right)
        } yield {
          Join(joinType, lateral, newLeft, newRight, on)
        }
      }
    )

    newFroms.filterNot(_ == select.from).map { newFrom => select.copy(from = newFrom) }
  }

  private def rollupAtomicFrom(from: AtomicFrom): Seq[AtomicFrom] = {
    from match {
      case FromStatement(stmt, label, resourceName, alias) =>
        rollup(stmt).map(FromStatement(_, label, resourceName, alias))
      case other =>
        Nil
    }
  }

  // see if there are rollups that match `select ${all the columns} from $from`...
  private def rollupPrefix(from: Join, demandedColumns: Map[Col, CT]): Seq[(FromStatement, Map[Col, VirtCol])] = {
    val ord1 = Ordering[(Int, Int)]
    val ord2 = Ordering[(Int, MT#DatabaseColumnNameImpl)]
    val orderedDemandedColumns: Seq[(Col, CT)] = demandedColumns.toSeq.
      sortWith { case ((col1, _ct1), (col2, _ct2)) =>
        (col1, col2) match {
          case (VirtCol(AutoTableLabel(atl1), AutoColumnLabel(acl1)),
                VirtCol(AutoTableLabel(atl2), AutoColumnLabel(acl2))) =>
            ord1.compare((atl1, acl1), (atl2, acl2)) < 0
          case (PhysCol(AutoTableLabel(atl1), _, _, DatabaseColumnName(dtn1)),
                PhysCol(AutoTableLabel(atl2), _, _, DatabaseColumnName(dtn2))) =>
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
  private case class PhysCol(table: AutoTableLabel, tableName: DatabaseTableName, canonicalName: CanonicalName, column: DatabaseColumnName) extends Col {
    def at(typ: CT, pos: AtomicPositionInfo) = PhysicalColumn(table, tableName, canonicalName, column, typ)(pos)
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
          case PhysicalColumn(table, tableName, canonName, col, typ) if from(table) =>
            acc += PhysCol(table, tableName, canonName, col) -> typ
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
        case pc@PhysicalColumn(table, tableName, canonName, col, typ) =>
          columnMap.get(PhysCol(table, tableName, canonName, col)) match {
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
