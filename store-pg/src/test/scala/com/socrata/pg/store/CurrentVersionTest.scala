package com.socrata.pg.store

import scala.language.reflectiveCalls

class CurrentVersionTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  import PGSecondaryUtil._

  test("handle CurrentVersion") {
    withPguUnconstrained { pgu =>
      val f = columnsCreatedFixture

      var version = f.dataVersion
      f.events foreach { e =>
        version = version + 1
        f.pgs.doVersion(pgu, f.datasetInfo, version, version, None, Iterator(e), Nil)
      }

      f.pgs.doCurrentVersion(pgu, f.datasetInfo.internalName, None) shouldEqual version
    }
  }

  test("handle CurrentVersion over a range") {
    withPguUnconstrained { pgu =>
      val f = columnsCreatedFixture

      var version = f.dataVersion
      f.events foreach { e =>
        f.pgs.doVersion(pgu, f.datasetInfo, version + 1, version + 10, None, Iterator(e), Nil)
        version = version + 10
      }

      f.pgs.doCurrentVersion(pgu, f.datasetInfo.internalName, None) shouldEqual version
    }
  }


}
