package com.socrata.pg.store

import scala.language.reflectiveCalls

class CurrentVersionTest extends PGSecondaryTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle CurrentVersion") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      var version = f.dataVersion
      f.events foreach { e =>
        version = version + 1
        f.pgs._version(pgu, f.datasetInfo, version, None, Iterator(e))
      }

      assert(f.pgs._currentVersion(pgu, testInternalName, None) == version)

    }
  }


}
