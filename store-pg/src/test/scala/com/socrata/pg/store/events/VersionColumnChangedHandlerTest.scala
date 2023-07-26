package com.socrata.pg.store.events

import com.socrata.soql.environment.ColumnName

import scala.language.reflectiveCalls

import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.secondary.{ColumnCreated, ColumnInfo, VersionColumnChanged}
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverseTestBase, PGStoreTestBase}
import com.socrata.soql.types.SoQLID

class VersionColumnChangedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("handle VersionColumnChanged") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLID, false, false, false, None)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLID, false, false, false, None))
      )
      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, events.iterator, Nil)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)

      schema.values.filter(_.isVersion) should have size (1)
    }
  }

  test("VersionColumnChanged should refuse to run a second time") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":version")), SoQLID, false, false, false, None)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":version")), SoQLID, false, false, false, None)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":version")), SoQLID, false, false, false, None))
      )
      intercept[UnsupportedOperationException] {
        f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, events.iterator, Nil)
      }
    }
  }
}
