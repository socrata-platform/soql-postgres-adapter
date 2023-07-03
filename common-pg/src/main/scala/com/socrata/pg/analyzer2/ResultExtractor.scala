package com.socrata.pg.analyzer2

import scala.reflect.ClassTag

import java.sql.ResultSet

import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.analyzer2._

class ResultExtractor[MT <: MetaTypes](
  rawSchema: OrderedMap[types.ColumnLabel[MT], AugmentedType[MT]],
  repFor: Rep.Provider[MT]
)(implicit tag: ClassTag[MT#ColumnValue]) extends SqlizerUniverse[MT] {
  private val extractors = rawSchema.valuesIterator.map { case AugmentedType(typ, isExpanded) =>
    repFor(typ).extractFrom(isExpanded)
  }.toArray

  val schema: OrderedMap[ColumnLabel, CT] =
    OrderedMap() ++ rawSchema.iterator.map { case (c, AugmentedType(t, v)) => c -> t }

  def extractRow(rs: ResultSet): Array[CV] = {
    val result = new Array[CV](extractors.length)

    var i = 0
    var dbCol = 1
    while(i != result.length) {
      val (width, value) = extractors(i)(rs, dbCol)

      result(i) = value
      i += 1
      dbCol += width
    }

    result
  }

  def fetchSize =
    if(schema.valuesIterator.exists(repFor(_).isPotentiallyLarge)) {
      10
    } else {
      1000
    }
}
