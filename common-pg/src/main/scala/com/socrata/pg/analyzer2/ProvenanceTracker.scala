package com.socrata.pg.analyzer2

import scala.collection.{mutable => scm}
import scala.collection.compat.immutable
import scala.{collection => sc}

import com.socrata.soql.analyzer2._

class ProvenanceTracker[MT <: MetaTypes] private (exprs: sc.Map[Expr[MT], Set[Option[CanonicalName]]]) extends SqlizerUniverse[MT] {
  // Returns the set of possible origin-tables for the value of this
  // expression.  In a tree where this is size <2 but demanded by an
  // expression with size >=2 is where provenances should start
  // getting tracked dynamically.
  def apply(expr: Expr): Set[Option[CanonicalName]] =
    exprs(expr)

  def isSingleton(expr: Expr): Boolean = {
    val set = this(expr)
    ProvenanceTracker.isSingleton(set)
  }
}

object ProvenanceTracker {
  def isSingleton(set: Set[Option[CanonicalName]]): Boolean = {
    set.size == 1 && !set(Some(rollup.RollupRewriter.MAGIC_ROLLUP_CANONICAL_NAME))
  }

  def apply[MT <: MetaTypes](s: Statement[MT], provenanceOf: LiteralValue[MT] => Set[Option[CanonicalName]]): ProvenanceTracker[MT] = {
    val builder = new Builder[MT](provenanceOf)
    builder.processStatement(s)
    new ProvenanceTracker(builder.exprs)
  }

  private class Builder[MT <: MetaTypes](provenanceOf: LiteralValue[MT] => Set[Option[CanonicalName]]) extends SqlizerUniverse[MT] {
    val exprs = new scm.HashMap[Expr, Set[Option[CanonicalName]]]
    val identifiers = new scm.HashMap[(AutoTableLabel, ColumnLabel), Set[Option[CanonicalName]]]

    private val emptySLR = immutable.ArraySeq[Set[Option[CanonicalName]]]()

    def processStatement(s: Statement): Seq[Set[Option[CanonicalName]]] = {
      s match {
        case CombinedTables(op, left, right) =>
          (processStatement(left), processStatement(right)).zipped.map(_ union _)
        case CTE(defLabel, _defAlias, defQuery, _, useQuery) =>
          processStatement(defQuery)
          processStatement(useQuery)

        case Values(labels, values) =>
          val result = Array.fill(labels.size)(Set.empty[Option[CanonicalName]])
          for(row <- values) {
            for((col, i) <- row.iterator.zipWithIndex) {
              val prov = processExpr(col, emptySLR)
              result(i) ++= prov
            }
          }
          immutable.ArraySeq.unsafeWrapArray(result) // safety: nothing else has a reference to "result"

        case Select(distinctiveness, selectList, from, where, groupBy, having, orderBy, limit, offset, search, hint) =>
          // Doing From first to populate identifiers
          processFrom(from)

          // Then selectList so that provs can be attached to selectListProvenances
          val selectListProvenances = locally {
            val result = new Array[Set[Option[CanonicalName]]](selectList.size)
            for((namedExpr, idx) <- selectList.valuesIterator.zipWithIndex) {
              result(idx) = processExpr(namedExpr.expr, emptySLR)
            }

            immutable.ArraySeq.unsafeWrapArray(result) // safety: nothing else has a reference to "result"
          }

          // now all the rest
          distinctiveness match {
            case Distinctiveness.Indistinct() | Distinctiveness.FullyDistinct() =>
              // nothing to do
            case Distinctiveness.On(exprs) =>
              for(e <- exprs) {
                processExpr(e, selectListProvenances)
              }
          }

          for(w <- where) {
            processExpr(w, emptySLR) // where can't use selectlistferences
          }

          for(gb <- groupBy) {
            processExpr(gb, selectListProvenances)
          }

          for(h <- having) {
            processExpr(h, emptySLR) // having can't use selectlistferences
          }

          for(ob <- orderBy) {
            processExpr(ob.expr, selectListProvenances)
          }

          selectListProvenances
      }
    }

    def processFrom(f: From): Unit = {
      f.reduce[Unit](
        processAtomicFrom(_),
        { case ((), join) =>
          processAtomicFrom(join.right)
          processExpr(join.on, emptySLR)
        }
      )
    }

    def processAtomicFrom(af: AtomicFrom): Unit = {
      af match {
        case _ : FromSingleRow =>
          // no structure here
        case ft: FromTable =>
          val prov = Set[Option[CanonicalName]](Some(ft.canonicalName))
          for(col <- ft.columns.keysIterator) {
            identifiers += (ft.label, col) -> prov
          }
        case fs: FromStatement =>
          (processStatement(fs.statement), fs.schema).zipped.foreach { (prov, schemaInfo) =>
            identifiers += (schemaInfo.table, schemaInfo.column) -> prov
          }
      }
    }

    def processExpr(e: Expr, selectList: immutable.ArraySeq[Set[Option[CanonicalName]]]): Set[Option[CanonicalName]] = {
      val prov: Set[Option[CanonicalName]] =
        e match {
          case l: LiteralValue =>
            provenanceOf(l)
          case c: Column =>
            identifiers((c.table, c.column))
          case n: NullLiteral =>
            Set.empty
          case FunctionCall(_func, args) =>
            args.foldLeft(Set.empty[Option[CanonicalName]]) { (acc, arg) =>
              acc ++ processExpr(arg, emptySLR) // selectlistreferences must be top-level
            }
          case AggregateFunctionCall(_func, args, _distinct, filter) =>
            val argProvs =
              args.foldLeft(Set.empty[Option[CanonicalName]]) { (acc, arg) =>
                acc ++ processExpr(arg, emptySLR)
              }
            filter.foldLeft(argProvs) { (acc, filter) =>
              acc ++ processExpr(filter, emptySLR)
            }
          case WindowedFunctionCall(_func, args, filter, partitionBy, orderBy, _frame) =>
            val argProv =
              args.foldLeft(Set.empty[Option[CanonicalName]]) { (acc, arg) =>
                acc ++ processExpr(arg, emptySLR)
              }
            val filterProv =
              filter.foldLeft(argProv) { (acc, filter) =>
                acc ++ processExpr(filter, emptySLR)
              }
            val partitionByProv =
              partitionBy.foldLeft(filterProv) { (acc, partitionBy) =>
                acc ++ processExpr(partitionBy, emptySLR)
              }
            orderBy.foldLeft(partitionByProv) { (acc, ob) =>
              acc ++ processExpr(ob.expr, emptySLR)
            }
          case SelectListReference(i, _, _, _) =>
            selectList(i - 1)
        }

      exprs(e) = prov

      prov
    }
  }
}
