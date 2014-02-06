package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}
import com.socrata.datacoordinator.secondary.{SystemRowIdentifierChanged, ColumnCreated, ColumnInfo}
import com.socrata.pg.store.PGSecondaryTestBase
import com.socrata.soql.types.SoQLID
import scala.language.reflectiveCalls

class SystemRowIdentifierChangedHandlerTest extends PGSecondaryTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle SystemRowIdentifierChanged") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false))
      )
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)
    }
  }



}
