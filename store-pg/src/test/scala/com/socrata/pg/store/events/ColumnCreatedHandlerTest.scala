package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryUniverseTestBase, PGStoreTestBase, PGSecondaryTestBase}
import com.socrata.datacoordinator.secondary.ColumnCreated

class ColumnCreatedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("handle ColumnCreated") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, f.events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)

      schema.values.map { ci => (ci.userColumnId, ci.systemId) } should contain theSameElementsAs
        f.events.collect { case ColumnCreated(ci) => (ci.id, ci.systemId) }
    }
  }
}
