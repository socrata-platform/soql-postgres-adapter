package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{WorkingCopyPublished}
import com.socrata.pg.store.PGSecondaryTestBase
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.truth.metadata.LifecycleStage

class WorkingCopyPublishedHandlerTest extends PGSecondaryTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle WorkingCopyPublished") {
    withPgu() { pgu =>
      val f = workingCopyCreatedFixture
      val events = f.events ++ Seq(
        WorkingCopyPublished
      )
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)

      truthCopyInfo.lifecycleStage shouldEqual LifecycleStage.Published
    }
  }

}
