package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.secondary.{FieldNameUpdated, ColumnCreated}
import com.socrata.pg.store.{PGStoreTestBase, PGSecondaryUniverseTestBase, PGSecondaryTestBase}
import com.socrata.soql.environment.ColumnName
import scala.language.reflectiveCalls

class FieldNameUpdatedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("Handle FieldNameUpdated") {
    withPgu() { pgu =>
      val f = fieldNameUpdatedFixture

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, f.events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)


      val expected = scala.collection.mutable.Map[UserColumnId, (ColumnId, Option[ColumnName])]()

      f.events.foreach {
        case ColumnCreated(ci) => expected.update(ci.id, (ci.systemId, ci.fieldName))
        case FieldNameUpdated(ci) => expected.update(ci.id, (ci.systemId, ci.fieldName))
        case _ =>
      }

      val expectedColumns = expected.map { p => (p._1, p._2._1, p._2._2) }

      schema.values.map { ci => (ci.userColumnId, ci.systemId, ci.fieldName) } should contain theSameElementsAs expectedColumns
    }
  }
}
