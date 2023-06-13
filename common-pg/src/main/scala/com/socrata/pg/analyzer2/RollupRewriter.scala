package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._

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
  class Noop[MT <: MetaTypes] extends RollupRewriter[MT] {
    protected def attemptRollups(analysis: SoQLAnalysis[MT]): Option[SoQLAnalysis[MT]] =
      None
  }

  def fromPostgres[MT <: MetaTypes](pgu: PGSecondaryUniverse[MT#ColumnType, MT#ColumnValue]) =
    new Noop[MT]
}
