package com.socrata.pg.store

import com.rojoma.simplearm.util.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, CopyId, RollupName, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.store.events.{RollupCreatedOrUpdatedHandler, WorkingCopyCreatedHandler, WorkingCopyPublishedHandler}
import com.socrata.soql.types._
import org.joda.time.DateTime

class RollupTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  // Despite being in the store project, it is getting its mutation script in the query project.
  override val project = "soql-server-pg"

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
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"), "select * (EXCEPT _point, _multipolygon)")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, copyInfo.dataVersion)

      val tableName = RollupManager.rollupTableName(rollupInfo, copyInfo.dataVersion)

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
        "select _make, avg(_aspect_ratio) AS _avg_asp WHERE _v_min >20 GROUP BY _make HAVING _avg_asp > 4 ORDER BY _make limit 100 offset 1")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, copyInfo.dataVersion)

      val tableName = RollupManager.rollupTableName(rollupInfo, copyInfo.dataVersion)
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

      val tableName = RollupManager.rollupTableName(rollupInfo, copyInfo.dataVersion)
      jdbcColumnCount(pgu.conn, tableName) should be (0)
      secondary.shutdown()
    }
  }

  test("rollup is copied from first to second copy") {
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
      val firstCopyPublished = getTruthCopyInfo(pgu, datasetInfo)
      firstCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val rollup = RollupInfo("ru", "select count(1)")
      RollupCreatedOrUpdatedHandler(pgu, firstCopy, rollup)

      val rollupsInFirstCopy = pgu.datasetMapReader.rollups(firstCopy)
      rollupsInFirstCopy.size should be (1)
      val rollupInFirstCopy = rollupsInFirstCopy.head
      RollupManager.makeSecondaryRollupInfo(rollupInFirstCopy) should be (rollup)

      // create the rollup table
      (new RollupManager(pgu, firstCopy)).updateRollup(rollupInFirstCopy, firstCopy.dataVersion)

      val tableNameFirstCopy = RollupManager.rollupTableName(rollupInFirstCopy, firstCopy.dataVersion)
      jdbcColumnCount(pgu.conn, tableNameFirstCopy) should be (1)
      jdbcRowCount(pgu.conn, tableNameFirstCopy) should be (1)

      val secondCopyInfo = CopyInfo(copyId, 2, LifecycleStage.Unpublished, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, datasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)

      val rollupsInSecondCopy = pgu.datasetMapReader.rollups(secondCopy)
      rollupsInSecondCopy.size should be (1)
      val rollupInSecondCopy = rollupsInSecondCopy.head
      RollupManager.makeSecondaryRollupInfo(rollupInSecondCopy) should be (rollup)

      val tableNameSecondCopy = RollupManager.rollupTableName(rollupInSecondCopy, secondCopy.dataVersion)
      jdbcColumnCount(pgu.conn, tableNameSecondCopy) should be (1)
      jdbcRowCount(pgu.conn, tableNameSecondCopy) should be (1)

      val allCopies = getTruthCopies(pgu, datasetInfo).toSeq
      allCopies should be (Seq(firstCopyPublished, secondCopy))
    }
  }

  test("rollup survives resync") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo("monkey", "locale", "obfuscate".getBytes)
      val secondaryCopyInfo = CopyInfo(new CopyId(123), 1, LifecycleStage.Published, 55, new DateTime())
      val cookie = Option("monkey")

      val newSchema = ColumnIdMap[ColumnInfo[SoQLType]](
        new ColumnId(10) -> ColumnInfo[SoQLType](new ColumnId(10), new UserColumnId(":id"), SoQLID, true, false, false),
        new ColumnId(11) -> ColumnInfo[SoQLType](new ColumnId(11), new UserColumnId(":version"), SoQLVersion, false, false, true),
        new ColumnId(12) -> ColumnInfo[SoQLType](new ColumnId(12), new UserColumnId("col"), SoQLText, false, false, false)
      )

      val rows = Seq(
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(10),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("foo")),
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(11),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("bar"))
      )

      // First resync a dataset that doesn't exist
      pgs._resync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator), Seq.empty)

      // there are 2 rows
      val truthCopyInfo = getTruthCopyInfo(pgu, secondaryDatasetInfo)
      for {
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 2

      // add a rollup
      val rollup = RollupInfo("ru", "select _col")
      RollupCreatedOrUpdatedHandler(pgu, truthCopyInfo, rollup)
      val rm = new RollupManager(pgu, truthCopyInfo)
      pgu.datasetMapReader.rollups(truthCopyInfo).foreach { ru =>
        rm.updateRollup(ru, truthCopyInfo.dataVersion)
      }

      val rows2 = rows :+
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(12),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("taz"))

      // resync again with an extra row plus rollup
      pgs._resync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator), Seq(rollup))

      // there is rollup
      val truthCopyInfoAfterResync = getTruthCopyInfo(pgu, secondaryDatasetInfo)
      val rollups = pgu.datasetMapReader.rollups(truthCopyInfoAfterResync)
      rollups.size shouldEqual 1
      rollups.foreach { ru =>
        val rollupTableName = RollupManager.rollupTableName(ru, truthCopyInfoAfterResync.dataVersion)
        jdbcColumnCount(pgu.conn, rollupTableName) should be (1)
        jdbcRowCount(pgu.conn, rollupTableName) should be (rows2.size)
      }

      // there are 3 rows
      for {
        reader <- pgu.datasetReader.openDataset(truthCopyInfoAfterResync)
        rows <- reader.rows()
      } rows.size shouldEqual 3
    }
  }
}
