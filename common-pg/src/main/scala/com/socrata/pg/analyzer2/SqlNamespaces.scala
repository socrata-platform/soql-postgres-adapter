package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

trait SqlNamespaces[MT <: MetaTypes] extends LabelUniverse[MT] {
  private var counter = 0L

  // Returns an identifier that is guaranteed not to conflict with any
  // other identifier that this query could use.
  def gensym(): Doc[Nothing] = {
    counter += 1
    d"g$counter" // "g" for "gensym"
  }

  // Turns an AutoTableLabel into a table name that is guaranteed to
  // not conflict with any real table.
  def tableLabel(table: AutoTableLabel): Doc[Nothing] =
    Doc(SqlNamespaces.tableLabel(table))

  // If the label is an AutoColumnLabel, turns it into a column name
  // that is guaranteed to not conflict with any real column.
  // Otherwise it returns the (base of) the physical column(s) which
  // make up that logical column.
  def columnBase(label: ColumnLabel): Doc[Nothing] =
    label match {
      case dcn: DatabaseColumnName => databaseColumnBase(dcn)
      case acl: AutoColumnLabel => Doc(SqlNamespaces.columnBase(acl))
    }

  def databaseTableName(dtn: DatabaseTableName): Doc[Nothing]
  def databaseColumnBase(dcn: DatabaseColumnName): Doc[Nothing]

  def indexName(dtn: DatabaseTableName, col: ColumnLabel): Doc[Nothing] =
    d"idx_" ++ databaseTableName(dtn) ++ d"_" ++ columnBase(col)

  def indexName(dtn: DatabaseTableName, col: ColumnLabel, subcol: String): Doc[Nothing] =
    indexName(dtn, col) ++ d"_" ++ Doc(subcol)
}

object SqlNamespaces {
  def tableLabel(table: AutoTableLabel): String =
    s"x${table.name}" // "x" because "t" is taken by the physical tables

  def columnBase(label: AutoColumnLabel): String = {
    val AutoColumnLabel(n: Int) = label
    s"i$n" // "i" for "intermediate"
  }
}
