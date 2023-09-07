package com.socrata.pg.analyzer2

import scala.collection.immutable.ListSet
import scala.collection.compat._

import com.socrata.soql.analyzer2._

class TransformManager[MT <: MetaTypes] private (
  analysis: SoQLAnalysis[MT],
  rollups: Seq[rollup.RollupInfo[MT]],
  rewritePasses: RewritePasses[MT],
  rollupExact: rollup.RollupExact[MT],
  rolledUp: ListSet[TransformManager.WrappedSoQLAnalysis[MT]]
)(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]) extends SqlizerUniverse[MT] {
  def allAnalyses = Vector(analysis) ++ rolledUp

  def applyPasses(passes: Seq[rewrite.Pass]): TransformManager = {
    val newAnalysis =
      analysis.applyPasses(
        passes,
        rewritePasses.isLiteralTrue,
        rewritePasses.isOrderable,
        rewritePasses.and
      )
    val newRollups = rolledUp ++ TransformManager.doRollup(newAnalysis, rollupExact, rollups)

    new TransformManager(
      newAnalysis,
      rollups,
      rewritePasses,
      rollupExact,
      newRollups
    )
  }
}

object TransformManager {
  def apply[MT <: MetaTypes](
    analysis: SoQLAnalysis[MT],
    rollups: Seq[rollup.RollupInfo[MT]],
    rewritePasses: RewritePasses[MT],
    rollupExact: rollup.RollupExact[MT]
  )(implicit ordering: Ordering[MT#DatabaseColumnNameImpl]): TransformManager[MT] = {
    new TransformManager(
      analysis,
      rollups,
      rewritePasses,
      rollupExact,
      TransformManager.doRollup(analysis, rollupExact, rollups).to(ListSet)
    )
  }

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
