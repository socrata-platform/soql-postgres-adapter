package com.socrata.pg.analyzer2

import scala.collection.immutable.ListSet
import scala.collection.compat._

import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._

final abstract class TransformManager

object TransformManager {
  private val log = LoggerFactory.getLogger(classOf[TransformManager])

  def apply[MT <: MetaTypes](
    analysis: SoQLAnalysis[MT],
    rollups: Seq[rollup.RollupInfo[MT]],
    passes: Seq[Seq[rewrite.Pass]],
    rewritePassHelpers: RewritePassHelpers[MT],
    rollupExact: rollup.RollupExact[MT],
    stringifier: Stringifier[MT]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): Vector[SoQLAnalysis[MT]] = {
    log.debug("Rewriting query:\n  {}", stringifier.statement(analysis.statement).indent(2))

    val initialRollups = doRollup(analysis, rollupExact, rollups)
    log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(initialRollups, stringifier)).indent(2))

    val (resultAnalysis, rolledUp) =
      passes.foldLeft(
        (analysis, initialRollups)
      ) { case ((analysis, rolledUp), passes) =>
          val newAnalysis =
            analysis.applyPasses(
              passes,
              rewritePassHelpers.isLiteralTrue,
              rewritePassHelpers.isOrderable,
              rewritePassHelpers.and
            )

          val rewrittenRollups = rolledUp.map { wsa =>
            new WrappedSoQLAnalysis(wsa.analysis)
          }

          val newRollups = rewrittenRollups ++ doRollup(newAnalysis, rollupExact, rollups)

          log.debug("After {}:\n  {}", passes: Any, stringifier.statement(newAnalysis.statement).indent(2))
          log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(newRollups, stringifier)).indent(2))

          (newAnalysis, newRollups)
      }

    Vector(resultAnalysis) ++ rolledUp.map(_.analysis)
  }

  private def printRollups[MT <: MetaTypes](rollups: Iterable[WrappedSoQLAnalysis[MT]], stringifier: Stringifier[MT]): String =
    rollups.iterator.map { wsa => stringifier.statement(wsa.analysis.statement) }.mkString(";\n")

  private class WrappedSoQLAnalysis[MT <: MetaTypes](val analysis: SoQLAnalysis[MT]) {
    override def hashCode = analysis.statement.hashCode
    override def equals(o: Any) =
      o match {
        case that: WrappedSoQLAnalysis[_] => this.analysis.statement == that.analysis.statement
        case _ => false
      }
  }

  private def doRollup[MT <: MetaTypes](
    analysis: SoQLAnalysis[MT],
    rollupExact: rollup.RollupExact[MT],
    rollups: Seq[rollup.RollupInfo[MT]]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]) =
    analysis.modifySeq { (labelProvider, statement) =>
      val rr = new rollup.RollupRewriter(labelProvider, rollupExact, rollups)
      rr.rollup(statement)
    }.map(new WrappedSoQLAnalysis(_))
}
