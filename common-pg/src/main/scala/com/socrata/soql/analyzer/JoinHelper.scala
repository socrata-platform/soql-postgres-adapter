package com.socrata.soql.analyzer

import com.socrata.soql.{SoQLAnalysis, typed}

object JoinHelper {
  // joins are simple if there is no subAnalysis ("join @aaaa-aaaa[ as a]") or if all analyses in subAnalysis
  // are select * "join (select * from @aaaa-aaaa) as a"
  def isSimple(join: typed.Join[_, _]): Boolean = {
    join.from.subAnalysis.forall(_.analyses.forall(_.selection.keys.isEmpty))
  }


  def expandJoins[ColumnId, Type](join: typed.Join[ColumnId, Type]): List[typed.Join[ColumnId, Type]] = {
    if (isSimple(join)) List(join)
    else expandJoins(join.from.analyses) :+ join
  }

  def expandJoins[ColumnId, Type](analyses: List[SoQLAnalysis[ColumnId, Type]]): List[typed.Join[ColumnId, Type]] = {
    analyses.flatMap(_.joins.flatMap(expandJoins))
  }
}
