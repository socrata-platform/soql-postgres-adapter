package com.socrata.pg.store

import scala.language.reflectiveCalls

class CurrentVersionTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  import PGSecondaryUtil._

  test("handle CurrentVersion") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      var version = f.dataVersion
      f.events foreach { e =>
        version = version + 1
        f.pgs.doVersion(pgu, f.datasetInfo, version, None, Iterator(e))
      }

      f.pgs.doCurrentVersion(pgu, f.datasetInfo.internalName, None) shouldEqual version
    }
  }


}
