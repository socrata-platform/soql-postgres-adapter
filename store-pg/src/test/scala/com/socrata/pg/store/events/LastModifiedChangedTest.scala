package com.socrata.pg.store.events

import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Random

import com.socrata.datacoordinator.secondary.LastModifiedChanged
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverseTestBase, PGStoreTestBase}
import org.joda.time.DateTime

class LastModifiedChangedTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("handle LastModifiedChanged") {
    withPguUnconstrained() { pgu =>
      val f = workingCopyCreatedFixture

      // Any random time within the last year
      val someRandomTime = DateTime.now().minusSeconds(Random.nextInt(60 * 60 * 24 * 365))
      val events = f.events ++ Seq(
        LastModifiedChanged(someRandomTime)
      )
      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, events.iterator, Nil)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      truthCopyInfo.lastModified should be (someRandomTime)
    }
  }
}
