package com.socrata.pg.store

import com.rojoma.simplearm.v2.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, CopyId, RollupName, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.store.events.{ColumnCreatedHandler, RollupCreatedOrUpdatedHandler, WorkingCopyCreatedHandler, WorkingCopyPublishedHandler}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import org.joda.time.DateTime

class RollupTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  override def beforeAll: Unit = {
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
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"),
        "select * (EXCEPT _point, _multipolygon, _location, _phone)")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, None, Function.const(false))

      val tableName = rollupInfo.tableName

      jdbcColumnCount(pgu.conn, tableName) should be (24)
      jdbcRowCount(pgu.conn,tableName) should be (totalRows)
      secondary.shutdown()
    }
  }

  test("rollup geometric columns") {
    withPgu() { pgu =>
      val secondary = new PGSecondary(config)
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"), "select _point, _multipolygon, _polygon, _line, _multipoint")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, None, Function.const(false))

      val tableName = rollupInfo.tableName

      jdbcColumnCount(pgu.conn, tableName) should be (5)
      jdbcRowCount(pgu.conn,tableName) should be (totalRows)
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
      rm.updateRollup(rollupInfo, None, Function.const(false))

      val tableName = rollupInfo.tableName
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
      rm.updateRollup(rollupInfo, None, Function.const(false))

      val tableName = rollupInfo.tableName
      jdbcColumnCount(pgu.conn, tableName) should be (0)
      secondary.shutdown()
    }
  }

  test("rollup is copied from first to second copy") {
    withPgu() { pgu =>

      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, None)
      val dataVersion = 0L
      val copyId = new CopyId(100)

      val copyInfo = CopyInfo(copyId, 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())

      WorkingCopyCreatedHandler(pgu, None, datasetInfo, copyInfo)

      val firstCopy = getTruthCopyInfo(pgu, datasetInfo)
      createSystemColumns(pgu, firstCopy, 100)

      firstCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      firstCopy.copyNumber should be (1L)

      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(datasetInfo.internalName)
      datasetId.isDefined should be (true)
      WorkingCopyPublishedHandler(pgu, firstCopy)
      val firstCopyPublished: TruthCopyInfo = getTruthCopyInfo(pgu, datasetInfo)
      firstCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val rollup = RollupInfo("ru", "select count(1)")
      RollupCreatedOrUpdatedHandler(pgu, firstCopy, rollup)

      val rollupsInFirstCopy = pgu.datasetMapReader.rollups(firstCopy)
      rollupsInFirstCopy.size should be (1)
      val rollupInFirstCopy = rollupsInFirstCopy.head
      RollupManager.makeSecondaryRollupInfo(rollupInFirstCopy) should be (rollup)

      // create the rollup table
      (new RollupManager(pgu, firstCopyPublished)).updateRollup(rollupInFirstCopy, None, Function.const(false))

      val tableNameFirstCopy = rollupInFirstCopy.tableName
      jdbcColumnCount(pgu.conn, tableNameFirstCopy) should be (1)
      jdbcRowCount(pgu.conn, tableNameFirstCopy) should be (1)

      val secondCopyInfo = CopyInfo(copyId, 2, LifecycleStage.Unpublished, dataVersion, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, datasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)
      createSystemColumns(pgu, secondCopy, 200)
      // Publish the 2nd copy
      WorkingCopyPublishedHandler(pgu, secondCopy)

      // Update rollups in the 2nd copy
      val secondCopyPublished = getTruthCopyInfo(pgu, datasetInfo)
      val rollupsInSecondCopy = pgu.datasetMapReader.rollups(secondCopyPublished)
      rollupsInSecondCopy.size should be (1)
      val rollupInSecondCopy = rollupsInSecondCopy.head
      (new RollupManager(pgu, secondCopyPublished)).updateRollup(rollupInSecondCopy, Some(secondCopy), Function.const(true))
      RollupManager.makeSecondaryRollupInfo(rollupInSecondCopy) should be (rollup)

      val tableNameSecondCopy = rollupInSecondCopy.tableName
      jdbcColumnCount(pgu.conn, tableNameSecondCopy) should be (1)
      jdbcRowCount(pgu.conn, tableNameSecondCopy) should be (1)

      val allCopies = getTruthCopies(pgu, datasetInfo).toVector
      allCopies.size should be (2)
      val firstSnapshot = firstCopyPublished.unanchored.copy(lifecycleStage = TruthLifecycleStage.Snapshotted)
      allCopies(0).unanchored should be (firstSnapshot)
      allCopies(1) should be (secondCopyPublished)
    }
  }

  test("rollup survives resync") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo(PGSecondaryUtil.testInternalName, "locale", "obfuscate".getBytes, None)
      val secondaryCopyInfo = CopyInfo(new CopyId(123), 1, LifecycleStage.Published, 55, 55, new DateTime())
      val cookie = Option("monkey")

      val newSchema = ColumnIdMap[ColumnInfo[SoQLType]](
        new ColumnId(10) -> ColumnInfo[SoQLType](new ColumnId(10), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None),
        new ColumnId(11) -> ColumnInfo[SoQLType](new ColumnId(11), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None),
        new ColumnId(12) -> ColumnInfo[SoQLType](new ColumnId(12), new UserColumnId("col"), Some(ColumnName("col")), SoQLText, false, false, false, None)
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
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator), Seq.empty, Seq.empty)

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
        rm.updateRollup(ru, None, Function.const(true))
      }

      val rows2 = rows :+
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(12),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("taz"))

      // resync again with an extra row plus rollup
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator), Seq(rollup), Seq.empty)

      // there is rollup
      val truthCopyInfoAfterResync = getTruthCopyInfo(pgu, secondaryDatasetInfo)
      val rollups = pgu.datasetMapReader.rollups(truthCopyInfoAfterResync)
      rollups.size shouldEqual 1
      rollups.foreach { ru =>
        val rollupTableName = ru.tableName
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

  test("rollup of previous published copy is still good after a working copy is dropped") {
    withPgu() { pgu =>

      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey, None)
      val dataVersion = 0L
      val copyId = new CopyId(100)

      val copyInfo = CopyInfo(copyId, 1, LifecycleStage.Published, dataVersion, dataVersion, new DateTime())

      WorkingCopyCreatedHandler(pgu, None, datasetInfo, copyInfo)

      val firstCopy = getTruthCopyInfo(pgu, datasetInfo)
      firstCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      firstCopy.copyNumber should be (1L)

      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(datasetInfo.internalName)
      datasetId.isDefined should be (true)
      createSystemColumns(pgu, firstCopy, 300)

      WorkingCopyPublishedHandler(pgu, firstCopy)
      val firstCopyPublished: TruthCopyInfo = getTruthCopyInfo(pgu, datasetInfo)
      firstCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val rollup = RollupInfo("ru", "select count(1)")
      RollupCreatedOrUpdatedHandler(pgu, firstCopy, rollup)

      val rollupsInFirstCopy = pgu.datasetMapReader.rollups(firstCopy)
      rollupsInFirstCopy.size should be (1)
      val rollupInFirstCopy = rollupsInFirstCopy.head
      RollupManager.makeSecondaryRollupInfo(rollupInFirstCopy) should be (rollup)

      // create the rollup table
      (new RollupManager(pgu, firstCopyPublished)).updateRollup(rollupInFirstCopy, None, Function.const(true))

      val tableNameFirstCopy = rollupInFirstCopy.tableName
      jdbcColumnCount(pgu.conn, tableNameFirstCopy) should be (1)
      jdbcRowCount(pgu.conn, tableNameFirstCopy) should be (1)

      val secondCopyInfo = CopyInfo(copyId, 2, LifecycleStage.Unpublished, dataVersion, dataVersion, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, datasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, datasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)

      createSystemColumns(pgu, secondCopy, 400)

      val pgs = new PGSecondary(config)
      pgs.doVersion(pgu, datasetInfo, secondCopy.dataVersion + 1, secondCopy.dataVersion + 1, None, Iterator(WorkingCopyDropped))

      // Check that the rollup in the previous copy is still good.
      val previousCopy = getTruthCopyInfo(pgu, datasetInfo)
      val previousRollups = pgu.datasetMapReader.rollups(previousCopy)
      previousRollups.size should be (1)
      val previousRollup = previousRollups.head

      val tableNamePreviousRollup = previousRollup.tableName
      jdbcColumnCount(pgu.conn, tableNamePreviousRollup) should be (1)
      jdbcRowCount(pgu.conn, tableNamePreviousRollup) should be (1)
    }
  }

  test("rollup window function") {
    withPgu() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"),
        "select _make, avg(_aspect_ratio) over(partition by _make)  AS _avg_asp")

      val rm = new RollupManager(pgu, copyInfo)
      rm.updateRollup(rollupInfo, None, Function.const(true))

      val tableName = rollupInfo.tableName
      jdbcColumnCount(pgu.conn, tableName) should be (2)
      jdbcRowCount(pgu.conn,tableName) should be (18)
      secondary.shutdown()
    }
  }

  /**
   * Rollups need system columns to process queries
   */
  private def createSystemColumns(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copy: TruthCopyInfo, baseId: Int) {
    val col1 = com.socrata.datacoordinator.secondary.ColumnInfo[SoQLType](
      new ColumnId(baseId), new UserColumnId(":id"), None, SoQLID, true, false, false, None
    )
    val col2 = com.socrata.datacoordinator.secondary.ColumnInfo[SoQLType](
      new ColumnId(baseId + 1), new UserColumnId(":version"), None, SoQLVersion, false, false, true, None
    )
    ColumnCreatedHandler(pgu, copy, col1)
    ColumnCreatedHandler(pgu, copy, col2)
  }
}
