package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{LifecycleStage, CopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.id.CopyId
import com.socrata.pg.store.PGSecondaryTestBase
import scala.language.reflectiveCalls

class WorkingCopyCreatedHandlerTest extends PGSecondaryTestBase {
  import com.socrata.pg.store.PGSecondaryUtil._

  val datasetInfo = SecondaryDatasetInfo(testInternalName, localeName, obfuscationKey)
  val dataVersion = 0L
  val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)

  test("can handle working copy event") {
    withPgu() { pgu =>
      val datasetInfo = SecondaryDatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      WorkingCopyCreatedHandler(pgu, datasetInfo, copyInfo)
    }
  }

  test("refuse to create copies with copy ids != 1; we do not support working copies") {
    val copyInfo = CopyInfo(new CopyId(-1), -1, LifecycleStage.Published, dataVersion)
    withPgu() { pgu =>
      intercept[UnsupportedOperationException] {
        WorkingCopyCreatedHandler(pgu,datasetInfo, copyInfo)
      }
    }
  }

}
