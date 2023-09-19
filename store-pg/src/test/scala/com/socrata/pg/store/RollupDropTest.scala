package com.socrata.pg.store

import com.rojoma.json.v3.ast.JArray
import com.socrata.pg.store.events.CopyDroppedHandler
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.loader.sql.messages.LogData.LifecycleStage

class RollupDropTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  override def beforeAll: Unit = {
    createDatabases()
  }

  test("delete dataset should delete all rollups in all copies") {
    withPguUnconstrained { pgu =>
      val (truthDatasetId, secDatasetId) = importDataset(pgu.conn)
      pgu.commit()
      val allCopies = createTwoCopiesAndRollup(pgu, truthDatasetId, secDatasetId, publishSecondCopy = true)
      dropDataset(pgu, truthDatasetId)
      cleanupDroppedTables(pgu)
      allCopies.foreach { copy =>
        val dataTableName = copy.dataTableName
        hasDataTables(pgu.conn, dataTableName, copy.datasetInfo) should be (false)
        hasRollupTables(pgu.conn, dataTableName) should be (false)
      }
    }
  }

  test("delete one copy should delete rollups for that copy") {
    withPguUnconstrained { pgu =>
      val (truthDatasetId, secDatasetId) = importDataset(pgu.conn)
      pgu.commit()
      val allCopies = createTwoCopiesAndRollup(pgu, truthDatasetId, secDatasetId, publishSecondCopy = false)
      val copy1 = allCopies(0)
      val copy2 = allCopies(1)
      CopyDroppedHandler(pgu, copy2)
      pgu.commit()
      cleanupDroppedTables(pgu)
      rollupTableExists(pgu, copy1)
      val dataTableName = copy2.dataTableName
      hasDataTables(pgu.conn, dataTableName, copy2.datasetInfo) should be (false)
      hasRollupTables(pgu.conn, dataTableName) should be (false)
    }
  }

  private def createTwoCopiesAndRollup(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                       truthDatasetId: DatasetId, secDatasetId: DatasetId,
                                       publishSecondCopy: Boolean): Seq[CopyInfo] = {
    val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
    val latestCopy: CopyInfo = pgu.datasetMapReader.latest(datasetInfo)
    createRollup(pgu, latestCopy)
    // create a second copy.  rollup should be copied to the second copy.
    withSoQLCommon(truthDataSourceConfig) { common =>
      processMutation(common, fixtureFile[JArray]("mutate-copy.json"), truthDatasetId)
      pushToSecondary(common, truthDatasetId, false)
      if (publishSecondCopy) {
        processMutation(common, fixtureFile[JArray]("mutate-publish.json"), truthDatasetId)
        pushToSecondary(common, truthDatasetId, false)
      }
    }



    pgu.commit()
    val allCopies = pgu.datasetMapReader.allCopies(datasetInfo).toSeq

    allCopies.size should be (2)
    allCopies.foreach { copy =>
      if (RollupManager.shouldMaterializeRollups(copy.lifecycleStage)) {
        rollupTableExists(pgu, copy)
      }
    }
    allCopies
  }

  private def rollupTableExists(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo): Unit = {
    val rollupTableName = pgu.datasetMapReader.rollups(copyInfo).map { rollupInfo =>
      val rollupTableName = rollupInfo.tableName
      val materialized = RollupManager.shouldMaterializeRollups(copyInfo.lifecycleStage)
      val expectedTable = if (materialized) 1 else 0
      jdbcColumnCount(pgu.conn, rollupTableName) should be (expectedTable)
      if (materialized) {
        jdbcRowCount(pgu.conn, rollupTableName) should be (totalRows)
      }
      pgu.conn.commit()
      rollupTableName
    }
    rollupTableName.size should be (1)
  }
}
