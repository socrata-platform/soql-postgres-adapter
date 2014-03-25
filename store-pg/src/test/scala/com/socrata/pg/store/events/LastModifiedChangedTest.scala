package com.socrata.pg.store.events

import com.socrata.pg.store.{PGStoreTestBase, PGSecondaryTestBase}
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.secondary.{LastModifiedChanged}
import org.joda.time.DateTime
import scala.util.Random
import scala.language.postfixOps

class LastModifiedChangedTest extends PGSecondaryTestBase with PGStoreTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle LastModifiedChanged") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture

      // Any random time within the last year
      val someRandomTime = DateTime.now().minusSeconds(Random.nextInt(60 * 60 * 24 * 365))
      val events = f.events ++ Seq(
        LastModifiedChanged(someRandomTime)
      )
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)
      truthCopyInfo.lastModified should be (someRandomTime)
    }
  }
}
