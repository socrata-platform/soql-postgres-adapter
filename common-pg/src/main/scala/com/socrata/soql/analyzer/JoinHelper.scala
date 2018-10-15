package com.socrata.soql.analyzer

import com.socrata.soql.{SoQLAnalysis, typed}

object JoinHelper {
  // joins are simple if there is no subAnalysis ("join @aaaa-aaaa[ as a]") or if all analyses in subAnalysis
  // are select * "join (select * from @aaaa-aaaa) as a"
  def isSimple(join: typed.Join[_, _]): Boolean = {
    join.from.subAnalysis.forall(_.analyses.seq.forall(_.selection.keys.isEmpty))
  }

  def expandJoins[ColumnId, Type](analyses: Seq[SoQLAnalysis[ColumnId, Type]]): Seq[typed.Join[ColumnId, Type]] = {
    def expandJoins2(join: typed.Join[ColumnId, Type]): Seq[typed.Join[ColumnId, Type]] = {
      if (isSimple(join)) Seq(join)
      else expandJoins(join.from.analyses) :+ join
    }

    analyses.flatMap(_.joins.flatMap(expandJoins2))
  }
}
