package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

case class OrderBySql[MT <: MetaTypes](expr: ExprSql[MT], ascending: Boolean, nullLast: Boolean) extends SqlizerUniverse[MT] {
  private def addDirections(base: Doc): Doc = {
    var doc = base
    if(ascending) doc +#+= d"ASC"
    else doc +#+= d"DESC"
    if(nullLast) doc +#+= d"NULLS LAST"
    else doc +#+= d"NULLS FIRST"
    doc
  }

  def sqls: Seq[Doc] = expr.sqls.map(addDirections)
}
