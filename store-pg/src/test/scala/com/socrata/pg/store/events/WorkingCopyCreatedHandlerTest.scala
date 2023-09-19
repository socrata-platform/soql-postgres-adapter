package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{LifecycleStage, CopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.id.{DatasetId, CopyId}
import com.socrata.pg.store.{PGSecondaryUniverseTestBase, PGStoreTestBase, PGSecondaryTestBase}
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.truth.metadata
import org.joda.time.DateTime

class WorkingCopyCreatedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  val datasetInfo = SecondaryDatasetInfo(testInternalName, localeName, obfuscationKey, None)
  val dataVersion = 0L
  val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())

  test("can handle working copy event") {
    withPguUnconstrained { pgu =>
      val datasetInfo = SecondaryDatasetInfo(testInternalName, localeName, obfuscationKey, None)
      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(testInternalName)
      val dataVersion = 0L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, copyInfo)

      val truthCopyInfo = getTruthCopyInfo(pgu, datasetInfo)
      truthCopyInfo.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
    }
  }

  test("publish and create another working copy") {
    withPguUnconstrained { pgu =>
      val datasetInfo = SecondaryDatasetInfo(testInternalName, localeName, obfuscationKey, None)

      val dataVersion = 0L
      val copyId = new CopyId(100)

      val copyInfo = CopyInfo(copyId, 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())

      WorkingCopyCreatedHandler(pgu, None, datasetInfo, copyInfo)
      val firstCopy = getTruthCopyInfo(pgu, datasetInfo)
      firstCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      firstCopy.copyNumber should be (1L)

      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(datasetInfo.internalName)
      datasetId.isDefined should be (true)
      WorkingCopyPublishedHandler(pgu, firstCopy)
      val firstCopyPublished = getTruthCopyInfo(pgu, datasetInfo)
      firstCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val secondCopyInfo = CopyInfo(copyId, 2, LifecycleStage.Unpublished, dataVersion, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, datasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)

      val allCopies = getTruthCopies(pgu, datasetInfo).toSeq
      allCopies should be (Seq(firstCopyPublished, secondCopy))
    }
  }
}
