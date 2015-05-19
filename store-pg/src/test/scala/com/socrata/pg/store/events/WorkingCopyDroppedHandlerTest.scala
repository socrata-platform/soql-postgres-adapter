package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.CopyId
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}
import com.socrata.pg.store.{PGSecondary, PGStoreTestBase, PGSecondaryUniverseTestBase, PGSecondaryTestBase}
import com.socrata.pg.store.PGSecondaryUtil._
import org.joda.time.DateTime

class WorkingCopyDroppedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  // Despite being in the store project, it is getting its mutation script in the query project.
  override val project = "soql-server-pg"

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  test("dropping a working copy should bump data version of the previous published copy") {
    withPgu() { pgu =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 0L
      val copyId = new CopyId(100)

      val copyInfo = CopyInfo(copyId, 1, LifecycleStage.Published, dataVersion, new DateTime())

      WorkingCopyCreatedHandler(pgu, None, datasetInfo, copyInfo)

      val firstCopy = getTruthCopyInfo(pgu, datasetInfo)
      firstCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      firstCopy.copyNumber should be (1L)

      val datasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(testInternalName)
      datasetId.isDefined should be (true)
      WorkingCopyPublishedHandler(pgu, firstCopy)
      val firstCopyPublished: TruthCopyInfo = getTruthCopyInfo(pgu, datasetInfo)
      firstCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val secondCopyInfo = CopyInfo(copyId, 2, LifecycleStage.Unpublished, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, datasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)

      val pgs = new PGSecondary(config)
      pgs._version(pgu, datasetInfo, secondCopy.dataVersion + 1, None, Iterator(WorkingCopyDropped))

      val previousCopy = getTruthCopyInfo(pgu, datasetInfo)
      previousCopy.dataVersion should be (secondCopy.dataVersion + 1)
    }
  }
}