package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

class PostgresNamespaces extends SqlNamespaces[DatabaseNamesMetaTypes] {
  override def databaseTableName(dtn: DatabaseTableName) = {
    val DatabaseTableName(dataTableName) = dtn
    Doc(dataTableName.name)
  }

  override def databaseColumnBase(dcn: DatabaseColumnName) = {
    val DatabaseColumnName(physicalColumnBase) = dcn
    Doc(physicalColumnBase)
  }

  protected override def gensymPrefix: String = "g"

  protected override def idxPrefix: String ="idx"

  protected override def autoTablePrefix: String = "x" // "t" is taken by physical tables

  protected override def autoColumnPrefix: String = "i"
}
