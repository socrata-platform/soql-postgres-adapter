package com.socrata.pg.store.events

import scala.language.reflectiveCalls

import com.socrata.datacoordinator.secondary.WorkingCopyPublished
import com.socrata.datacoordinator.truth.metadata.LifecycleStage
import com.socrata.pg.store.{PGSecondaryUniverseTestBase, PGSecondaryTestBase, PGStoreTestBase, PGCookie}

class WorkingCopyPublishedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  test("handle WorkingCopyPublished") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        WorkingCopyPublished
      )
      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, PGCookie.default, events.iterator, false)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)

      truthCopyInfo.lifecycleStage shouldEqual LifecycleStage.Published
    }
  }

}
