package com.socrata.soql.analyzer

import com.socrata.soql.{SimpleSoQLAnalysis, SoQLAnalysis, typed}

object JoinHelper {

  def expandJoins[ColumnId, Type](join: typed.Join[ColumnId, Type]): Seq[typed.Join[ColumnId, Type]] = {
    if (SimpleSoQLAnalysis.isSimple(join.tableLike)) Seq(join)
    else expandJoins(join.tableLike) :+ join
  }

  def expandJoins[ColumnId, Type](analyses: Seq[SoQLAnalysis[ColumnId, Type]]): Seq[typed.Join[ColumnId, Type]] = {
    analyses.flatMap { x =>
      x.join.toSeq.flatten.flatMap { j =>
        expandJoins(j)
      }
    }
  }
}
