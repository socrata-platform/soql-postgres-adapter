package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance

object TestProvenanceMapper extends types.ProvenanceMapper[SqlizerTest.TestMT] {
  def toProvenance(dtn: types.DatabaseTableName[SqlizerTest.TestMT]): Provenance = {
    val DatabaseTableName(name) = dtn
    Provenance(name)
  }

  def fromProvenance(prov: Provenance): types.DatabaseTableName[SqlizerTest.TestMT] = {
    val Provenance(name) = prov
    DatabaseTableName(name)
  }
}
