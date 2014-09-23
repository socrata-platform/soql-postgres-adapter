package com.socrata.pg.store

import scala.language.reflectiveCalls

class CurrentCopyNumberTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  import PGSecondaryUtil._

  test("handle CurrentCopyNumber") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture

      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, f.events.iterator)

      val actualCopyNum = f.pgs._currentCopyNumber(pgu, testInternalName, None)

      // right now we only support a single copy of the dataset ... so this is a silly test!
      actualCopyNum shouldEqual 1

    }
  }


}
