package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}
import com.socrata.datacoordinator.secondary.{VersionColumnChanged, SystemRowIdentifierChanged, ColumnCreated, ColumnInfo}
import com.socrata.pg.store.{PGStoreTestBase, PGSecondaryTestBase}
import com.socrata.soql.types.SoQLID
import scala.language.reflectiveCalls

class VersionColumnChangedHandlerTest extends PGSecondaryTestBase with PGStoreTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle VersionColumnChanged") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":version"), SoQLID, false, false, false)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":version"), SoQLID, false, false, false))
      )
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)

      schema.values.filter(_.isVersion) should have size (1)
    }
  }

  test("VersionColumnChanged should refuse to run a second time") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        VersionColumnChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false))
      )
      intercept[UnsupportedOperationException] {
        f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)
      }
    }
  }
}
