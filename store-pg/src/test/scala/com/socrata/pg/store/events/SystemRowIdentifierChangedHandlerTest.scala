package com.socrata.pg.store.events

import com.socrata.soql.environment.ColumnName

import scala.language.reflectiveCalls

import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.secondary.{ColumnCreated, ColumnInfo, SystemRowIdentifierChanged}
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverseTestBase, PGStoreTestBase}
import com.socrata.soql.types.SoQLID

class SystemRowIdentifierChangedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("handle SystemRowIdentifierChanged") {
    withPgu { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")),  SoQLID, false, false, false, None))
      )
      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, events.iterator, Nil)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      val schema = pgu.datasetMapReader.schema(truthCopyInfo)

      schema.values.filter(_.isSystemPrimaryKey) should have size (1)
    }
  }

  test("SystemRowIdentifierChanged should refuse to run a second time") {
    withPgu { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, false, false, false, None))
      )
      intercept[UnsupportedOperationException] {
        f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, events.iterator, Nil)
      }
    }
  }
}
