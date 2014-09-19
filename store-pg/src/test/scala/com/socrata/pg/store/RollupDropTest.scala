package com.socrata.pg.store

import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.store.events.CopyDroppedHandler
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.DatasetId


class RollupDropTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase {

  val dcInstance = "alpha"

  val project: String = "soql-server-pg"

  val storeId: String = "pg"

  override def beforeAll = {
    createDatabases()
  }

  test("delete dataset should delete all rollups in all copies") {
    withPgu() { pgu =>
      val (truthDatasetId, secDatasetId) = importDataset(pgu.conn)
      pgu.commit()
      val allCopies = createTwoCopiesAndRollup(pgu, truthDatasetId, secDatasetId)
      dropDataset(pgu, truthDatasetId)
      cleanupDroppedtable(pgu)
      allCopies.foreach { copy =>
        val dataTableName = copy.dataTableName
        hasDataTables(pgu.conn, dataTableName, copy.datasetInfo) should be (false)
        hasRollupTables(pgu.conn, dataTableName) should be (false)
      }
    }
  }

  test("delete one copy should delete rollups for that copy") {
    withPgu() { pgu =>
      val (truthDatasetId, secDatasetId) = importDataset(pgu.conn)
      pgu.commit()
      val allCopies = createTwoCopiesAndRollup(pgu, truthDatasetId, secDatasetId)
      val copy1 = allCopies(0)
      val copy2 = allCopies(1)
      CopyDroppedHandler(pgu, copy2)
      pgu.commit()
      cleanupDroppedtable(pgu)
      rollupTableExists(pgu, copy1)
      val dataTableName = copy2.dataTableName
      hasDataTables(pgu.conn, dataTableName, copy2.datasetInfo) should be (false)
      hasRollupTables(pgu.conn, dataTableName) should be (false)
    }
  }

  private def createTwoCopiesAndRollup(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                       truthDatasetId: DatasetId, secDatasetId: DatasetId): Seq[CopyInfo] = {
    val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
    createRollup(pgu, pgu.datasetMapReader.latest(datasetInfo))
    // create a second copy.  rollup should be copied to the second copy.
    withSoQLCommon(truthDataSourceConfig) { common =>
      processMutation(common, fixtureFile("mutate-copy.json"), truthDatasetId)
      pushToSecondary(common, truthDatasetId, false)
    }
    pgu.commit()
    val allCopies = pgu.datasetMapReader.allCopies(datasetInfo).toSeq
    allCopies.size should be (2)
    allCopies.foreach(rollupTableExists(pgu, _))
    allCopies
  }

  private def rollupTableExists(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {
    val rollupTableName = pgu.datasetMapReader.rollups(copyInfo).map { rollupInfo =>
    val rollupTableName = RollupManager.rollupTableName(rollupInfo, copyInfo.dataVersion)
      jdbcColumnCount(pgu.conn, rollupTableName) should be (1)
      jdbcRowCount(pgu.conn, rollupTableName) should be (11)
      pgu.conn.commit()
      rollupTableName
    }
    rollupTableName.size should be (1)
  }
}
