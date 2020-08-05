package com.socrata.pg.store

import scala.language.reflectiveCalls

import PGSecondaryUtil._

class CurrentCopyNumberTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {
  test("handle CurrentCopyNumber") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, PGCookie.default, f.events.iterator, false)

      val actualCopyNum = f.pgs.doCurrentCopyNumber(pgu, f.datasetInfo.internalName, None)

      // right now we only support a single copy of the dataset ... so this is a silly test!
      actualCopyNum shouldEqual 1

    }
  }
}
