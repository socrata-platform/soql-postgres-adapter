package com.socrata.pg.store

import com.socrata.datacoordinator.id.RollupName
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.soql.types.{SoQLValue, SoQLType}

class RollupTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase {

  val dcInstance = "alpha"
  val project: String = "soql-server-pg"
  val storeId: String = "pg"

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  test("rollup all columns except point") {
    withPgu() { pgu =>
      val secondary = new PGSecondary(config)
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      // TODO MS fix up geometry columns so we can make rollups on them, low priority
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"), "select * (EXCEPT point)")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, copyInfo.dataVersion)

      val tableName = rm.rollupTableName(rollupInfo, copyInfo.dataVersion)

      jdbcColumnCount(pgu.conn, tableName) should be (20)
      jdbcRowCount(pgu.conn,tableName) should be (11)
      secondary.shutdown()
    }
  }

  test("rollup complex query") {
    withPgu() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"),
        "select make, avg(aspect_ratio) AS avg_asp WHERE v_min >20 GROUP BY make HAVING avg_asp > 4 ORDER BY make limit 100 offset 1")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, copyInfo.dataVersion)

      val tableName = rm.rollupTableName(rollupInfo, copyInfo.dataVersion)
      jdbcColumnCount(pgu.conn, tableName) should be (2)
      jdbcRowCount(pgu.conn,tableName) should be (2)
      secondary.shutdown()
    }
  }

  test("invalid soql shouldn't throw exception") {
    withPgu() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"),
        "select I am a monkey GROUP BY monkeyland")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, copyInfo.dataVersion)

      val tableName = rm.rollupTableName(rollupInfo, copyInfo.dataVersion)
      jdbcColumnCount(pgu.conn, tableName) should be (0)
      secondary.shutdown()
    }
  }
}
