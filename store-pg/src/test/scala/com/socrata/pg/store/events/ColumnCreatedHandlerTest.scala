package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryTestBase
import scala.language.reflectiveCalls

class ColumnCreatedHandlerTest extends PGSecondaryTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle ColumnCreated") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, f.events.iterator)
    }
  }


}
