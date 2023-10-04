package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

// This class is used when converting a SQL statement into a
// standardized form for use in rollups.  Because the intent is to
// just do a COPY into a freshly-created table with the output of this
// query, it does not attempt to preserve ordering.
object SqlNormalizer extends SqlizerUniverse[DatabaseNamesMetaTypes] {
  type Schema = OrderedMap[ColumnLabel, AugmentedType]
  def apply(partlyCompressed: Doc, schema: Schema): (Doc, OrderedMap[ColumnLabel, Rep])  = {
    if(schema.valuesIterator.forall { case AugmentedType(rep, isExpanded) => isExpanded || rep.expandedColumnCount == 1 }) {
      // we're already fully expanded
      (partlyCompressed, schema.withValuesMapped(_.rep))
    } else {
      // Ok what we'll be generating a SQL statement that looks like
      //    SELECT cols FROM (..doc..) AS toplevel
      // where "cols" are either the bare columns (if they're already expanded or are single-column
      // types) or are the extracted sub-columns otherwise.

      val selectLabel = "top_level"

      val reSelected: Seq[Doc] =
        schema.iterator.flatMap { case (label, AugmentedType(rep, isExpanded)) =>
          if(isExpanded) {
            rep.expandedDatabaseColumns(label).map { dbLabel =>
              Doc(selectLabel) ++ d"." ++ dbLabel
            }
          } else {
            rep.compressedSubColumns(selectLabel, label)
          }
        }.toSeq

      val resultQuery =
        (d"SELECT" +: reSelected.punctuate(d",")).vsep.nest(2).group ++ Doc.lineCat ++ (Seq(d"FROM (", partlyCompressed).vcat ++ d") AS" +#+ Doc(selectLabel)).group
      (resultQuery, schema.withValuesMapped(_.rep))
    }
  }
}
