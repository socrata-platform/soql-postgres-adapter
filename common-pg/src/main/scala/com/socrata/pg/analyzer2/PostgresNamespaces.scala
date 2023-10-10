package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

object PostgresNamespaces extends SqlNamespaces[DatabaseNamesMetaTypes] {
  override def rawDatabaseTableName(dtn: DatabaseTableName) = {
    val DatabaseTableName(dataTableName) = dtn
    dataTableName.name
  }

  override def rawDatabaseColumnBase(dcn: DatabaseColumnName) = {
    val DatabaseColumnName(physicalColumnBase) = dcn
    physicalColumnBase
  }

  override def gensymPrefix: String = "g"

  protected override def idxPrefix: String ="idx"

  protected override def autoTablePrefix: String = "x" // "t" is taken by physical tables

  protected override def autoColumnPrefix: String = "i"
}
