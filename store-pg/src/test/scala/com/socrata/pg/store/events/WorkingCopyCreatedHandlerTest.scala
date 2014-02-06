package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{LifecycleStage, CopyInfo, DatasetInfo}
import com.socrata.datacoordinator.id.CopyId
import com.socrata.pg.store.PGSecondaryTestBase

class WorkingCopyCreatedHandlerTest extends PGSecondaryTestBase {
  import com.socrata.pg.store.PGSecondaryUtil._

  test("can handle working copy event") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      WorkingCopyCreatedHandler(datasetInfo, dataVersion, copyInfo, conn)
    }
  }

  test("refuse to create working copies with copy ids != 1") {

  }

  test("fail when we create a new dataset with a copy id != 1") {

  }
}
