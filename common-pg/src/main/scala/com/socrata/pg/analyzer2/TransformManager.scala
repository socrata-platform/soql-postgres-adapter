package com.socrata.pg.analyzer2

import scala.collection.immutable.ListSet
import scala.collection.compat._

import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2._
import com.socrata.soql.util.LazyToString

import com.socrata.pg.store.RollupId

final abstract class TransformManager

object TransformManager {
  private val log = LoggerFactory.getLogger(classOf[TransformManager])

  def apply[MT <: MetaTypes](
    analysis: SoQLAnalysis[MT],
    rollups: Seq[rollup.RollupInfo[MT, RollupId]],
    passes: Seq[Seq[rewrite.Pass]],
    rewritePassHelpers: rewrite.RewritePassHelpers[MT],
    rollupExact: rollup.RollupExact[MT],
    stringifier: Stringifier[MT]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): Vector[(SoQLAnalysis[MT], Set[RollupId])] = {
    log.debug("Rewriting query:\n  {}", stringifier.statement(analysis.statement).indent(2))

    val preRollupPasses = passes.reverse.dropWhile(_.forall(_.category == rewrite.Pass.Category.Shallow)).reverse
    val postRollupPasses = passes.drop(preRollupPasses.length)

    val analysisAfterInitialPasses = preRollupPasses.foldLeft(analysis) { (analysis, passes) =>
      analysis.applyPasses(passes, rewritePassHelpers)
    }

    val initialRollups = doRollup(analysisAfterInitialPasses, rollupExact, rollups)
    log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(initialRollups, stringifier)).indent(2))

    val (resultAnalysis, rolledUp) =
      postRollupPasses.foldLeft(
        (analysisAfterInitialPasses, initialRollups)
      ) { case ((analysis, rolledUp), passes) =>
          val newAnalysis =
            analysis.applyPasses(passes, rewritePassHelpers)

          val newRollups = rolledUp ++ doRollup(newAnalysis, rollupExact, rollups)

          log.debug("After {}:\n  {}", passes: Any, stringifier.statement(newAnalysis.statement).indent(2))
          log.debug("Candidate rollups:\n  {}", LazyToString(printRollups(newRollups, stringifier)).indent(2))

          (newAnalysis, newRollups)
      }

    Vector((resultAnalysis, Set.empty[RollupId])) ++ rolledUp.map { wa =>
      (wa.analysis, wa.rollupIds)
    }
  }

  private def printRollups[MT <: MetaTypes](rollups: Iterable[WrappedSoQLAnalysis[MT]], stringifier: Stringifier[MT]): String =
    rollups.iterator.map { wsa => stringifier.statement(wsa.analysis.statement) }.mkString(";\n")

  private case class WrappedSoQLAnalysis[MT <: MetaTypes](analysis: SoQLAnalysis[MT], rollupIds: Set[RollupId])

  private def doRollup[MT <: MetaTypes](
    analysis: SoQLAnalysis[MT],
    rollupExact: rollup.RollupExact[MT],
    rollups: Seq[rollup.RollupInfo[MT, RollupId]]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): Seq[WrappedSoQLAnalysis[MT]] = {
    // ugh - modifySeq doesn't play nicely with returning additional
    // info in addition to the new analysis, so we need to pack that
    // away in a var here and then reassemble afterward.
    var rollupIdses = Seq.empty[Set[RollupId]]
    val newAnalyses =
      analysis.modifySeq { (labelProvider, statement) =>
        val rr = new rollup.RollupRewriter(labelProvider, rollupExact, rollups)
        val rewritten = rr.rollup(statement)
        rollupIdses = rewritten.map(_._2)
        rewritten.map(_._1)
      }.toVector
    newAnalyses.lazyZip(rollupIdses).map { (analysis, rollupIds) =>
      new WrappedSoQLAnalysis(analysis, rollupIds)
    }
  }
}
