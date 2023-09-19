package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryUniverseTestBase, PGStoreTestBase, PGSecondaryTestBase}
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.secondary.{ColumnCreated, ColumnRemoved}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}

class ColumnRemovedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("Handle ColumnRemoved") {
    withPgu { pgu =>
      val f = columnsRemovedFixture

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, f.events.iterator, Nil)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)

      val expectedColumns = scala.collection.mutable.ArrayBuffer[Tuple2[UserColumnId, ColumnId]]()

      f.events.foreach { event =>
        event match {
          case ColumnCreated(ci) => expectedColumns.+=: ((ci.id, ci.systemId))
          case ColumnRemoved(ci) => expectedColumns -= ((ci.id, ci.systemId))
          case _ =>
        }
      }

      schema.values.map { ci => (ci.userColumnId, ci.systemId) } should contain theSameElementsAs expectedColumns
    }
  }
}
