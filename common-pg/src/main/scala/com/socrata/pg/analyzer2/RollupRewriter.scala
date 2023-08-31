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
