package com.socrata.pg.store

import com.rojoma.simplearm.v2.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, CopyId, IndexId, IndexName, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.truth.metadata
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage => TruthLifecycleStage}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.store.events.{ColumnCreatedHandler, IndexCreatedOrUpdatedHandler, WorkingCopyCreatedHandler, WorkingCopyPublishedHandler}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import org.joda.time.DateTime

class IndexTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  override def beforeAll: Unit = {
    createDatabases()
    withDbUnconstrained() { conn =>
      importDataset(conn)
    }
  }

  test("create index") {
    withPguUnconstrained() { pgu =>
      val secondary = new PGSecondary(config)
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val indexInfo = pgu.datasetMapWriter.createOrUpdateIndex(copyInfo, new IndexName("idx1"), "make,code", None)
      val im = new IndexManager(pgu, copyInfo)
      im.updateIndex(indexInfo)
      val dbIndexes = jdbcIndexes(pgu.conn, copyInfo.dataTableName)
      val dbTableName = copyInfo.dataTableName
      val dbIndexName = im.dbIndexName(indexInfo)
      dbIndexes.contains(dbIndexName) should be(true)
      val indexRx = s"""CREATE INDEX ${dbIndexName} ON public.${dbTableName} USING btree \\(u_make_[0-9]+, u_code_[0-9]+\\)""".r
      indexRx.findFirstMatchIn(dbIndexes(dbIndexName)).isDefined should be(true)
      secondary.shutdown()
    }
  }

  test("create advanced index") {
    withPguUnconstrained() { pgu =>
      val secondary = new PGSecondary(config)
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val indexInfo = pgu.datasetMapWriter.createOrUpdateIndex(copyInfo, new IndexName("idx2"), "make,code || v_trim::text asc null last", Some("make is not null"))
      val im = new IndexManager(pgu, copyInfo)
      im.updateIndex(indexInfo)
      val dbIndexes = jdbcIndexes(pgu.conn, copyInfo.dataTableName)
      val dbTableName = copyInfo.dataTableName
      val dbIndexName = im.dbIndexName(indexInfo)
      dbIndexes.contains(dbIndexName) should be(true)
      val indexRx = s"""CREATE INDEX ${dbIndexName} ON public.${dbTableName} USING btree \\(u_make_[0-9]+, \\(\\(u_code_[0-9]+ || \\(\\(u_v_trim_[0-9]+\\)::character varying\\)::text\\)\\)\\) WHERE \\(u_make_[0-9]+ IS NOT NULL\\)""".r
      indexRx.findFirstMatchIn(dbIndexes(dbIndexName)).isDefined should be(true)
      secondary.shutdown()
    }
  }

  test("invalid expression shouldn't throw exception") {
    withPguUnconstrained() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)
      val indexInfo = pgu.datasetMapWriter.createOrUpdateIndex(copyInfo, new IndexName("idx_invalid"), "nonexist_column", None)
      val im = new IndexManager(pgu, copyInfo)
      im.updateIndex(indexInfo)
      val dbIndexes = jdbcIndexes(pgu.conn, copyInfo.dataTableName)
      val dbIndexName = im.dbIndexName(indexInfo)
      dbIndexes.contains(dbIndexName) should be(false)
      secondary.shutdown()
    }
  }

  test("index is copied from first to second copy") {
    withPguUnconstrained() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()

      pgu.datasetMapReader
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)

      val secDatasetInfo = DatasetInfo(datasetIdToInternal(truthDatasetId), localeName, obfuscationKey, None)
      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(datasetIdToInternal(truthDatasetId))

      val index = IndexInfo(new IndexId(1), "idx1", "make", None)
      val indexInfo = IndexCreatedOrUpdatedHandler(pgu, copyInfo, index)
      val indexesInFirstCopy = pgu.datasetMapReader.indexes(copyInfo)
      indexesInFirstCopy.size should be(1)
      val im = new IndexManager(pgu, copyInfo)
      im.updateIndex(indexesInFirstCopy.head)
      val dbIndexes = jdbcIndexes(pgu.conn, copyInfo.dataTableName)
      val dbIndexName = im.dbIndexName(indexInfo)
      dbIndexes.contains(dbIndexName) should be(true)
      val secondCopyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Unpublished, copyInfo.dataVersion + 1, copyInfo.dataVersion + 1, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, secDatasetInfo, secondCopyInfo)
      val secondCopy = getTruthCopyInfo(pgu, secDatasetInfo)
      secondCopy.lifecycleStage should be (metadata.LifecycleStage.Unpublished)
      secondCopy.copyNumber should be (2L)
      copyColumns(pgu, copyInfo, secondCopy,  200)
      // Publish the 2nd copy
      WorkingCopyPublishedHandler(pgu, secondCopy)
      val secondCopyPublished = pgu.datasetMapReader.latest(datasetInfo)
      secondCopyPublished.lifecycleStage should be (metadata.LifecycleStage.Published)

      val indexesInSecondPublished = pgu.datasetMapReader.indexes(secondCopyPublished)
      indexesInSecondPublished.size should be(1)
      val imSecondPublished = new IndexManager(pgu, secondCopyPublished)
      val indexSecondPublished = indexesInSecondPublished.head
      imSecondPublished.updateIndex(indexSecondPublished)
      val dbIndexesSecondPublished = jdbcIndexes(pgu.conn, secondCopyPublished.dataTableName)
      val dbIndexNameSecondPublished = imSecondPublished.dbIndexName(indexSecondPublished)
      dbIndexesSecondPublished.contains(dbIndexNameSecondPublished) should be(true)

      val allCopies = getTruthCopies(pgu, secDatasetInfo).toVector
      allCopies.size should be(2)
      val firstSnapshot = copyInfo.unanchored.copy(lifecycleStage = TruthLifecycleStage.Snapshotted)
      allCopies(0).unanchored should be(firstSnapshot)
      allCopies(1) should be(secondCopyPublished)
    }
  }

  test("index survives resync") {
    withPguUnconstrained() { pgu =>
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
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator), Seq.empty, Seq.empty, Nil)

      // there are 2 rows
      val truthCopyInfo = getTruthCopyInfo(pgu, secondaryDatasetInfo)
      for {
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 2

      // add an index
      val index = IndexInfo(new IndexId(-1), "idx1", "col desc", None)
      IndexCreatedOrUpdatedHandler(pgu, truthCopyInfo, index)
      val im = new IndexManager(pgu, truthCopyInfo)
      pgu.datasetMapReader.indexes(truthCopyInfo).foreach { idx =>
        im.updateIndex(idx)
      }

      val rows2 = rows :+
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(12),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("taz"))

      // resync again with an extra row plus rollup
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator), Nil, Nil, Seq(index))

      // there is index
      val truthCopyInfoAfterResync = getTruthCopyInfo(pgu, secondaryDatasetInfo)
      val indexes = pgu.datasetMapReader.indexes(truthCopyInfoAfterResync)
      val imAfterResync = new IndexManager(pgu, truthCopyInfoAfterResync)
      indexes.size shouldEqual 1
      val dbIndexesAfterResync = jdbcIndexes(pgu.conn, truthCopyInfoAfterResync.dataTableName)
      indexes.foreach { idx =>
        val dbIndexNameAfterResync = imAfterResync.dbIndexName(idx)
        dbIndexesAfterResync.contains(dbIndexNameAfterResync) should be(true)
      }

      // there are 3 rows
      for {
        reader <- pgu.datasetReader.openDataset(truthCopyInfoAfterResync)
        rows <- reader.rows()
      } rows.size shouldEqual 3
    }
  }

  test("index of previous published copy is still good after a working copy is dropped") {
    withPguUnconstrained() { pgu =>
      val secondary = new PGSecondary(config)
      secondary.shutdown()

      pgu.datasetMapReader
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val copyInfo = pgu.datasetMapReader.latest(datasetInfo)

      val secDatasetInfo = DatasetInfo(datasetIdToInternal(truthDatasetId), localeName, obfuscationKey, None)
      val datasetId = pgu.datasetMapReader.datasetIdForInternalName(datasetIdToInternal(truthDatasetId))

      val index = IndexInfo(new IndexId(1), "idx1", "make", None)
      val indexInfo = IndexCreatedOrUpdatedHandler(pgu, copyInfo, index)
      val indexesInFirstCopy = pgu.datasetMapReader.indexes(copyInfo)
      indexesInFirstCopy.size should be(1)
      val im = new IndexManager(pgu, copyInfo)
      im.updateIndex(indexesInFirstCopy.head)
      val dbIndexes = jdbcIndexes(pgu.conn, copyInfo.dataTableName)
      val dbIndexName = im.dbIndexName(indexInfo)
      dbIndexes.contains(dbIndexName) should be(true)
      val secondCopyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Unpublished, copyInfo.dataVersion + 1, copyInfo.dataVersion + 1, new DateTime())
      WorkingCopyCreatedHandler(pgu, datasetId, secDatasetInfo, secondCopyInfo)
      val secondUnpublished = getTruthCopyInfo(pgu, secDatasetInfo)
      secondUnpublished.lifecycleStage should be(metadata.LifecycleStage.Unpublished)
      secondUnpublished.copyNumber should be(2L)
      copyColumns(pgu, copyInfo, secondUnpublished,  200)

      val pgs = new PGSecondary(config)
      pgs.doVersion(pgu, secDatasetInfo, secondUnpublished.dataVersion + 1, secondUnpublished.dataVersion + 1, None, Iterator(WorkingCopyDropped), Nil)

      // Check that the index in the previous copy is still good.
      val previousCopy = getTruthCopyInfo(pgu, secDatasetInfo)
      previousCopy.lifecycleStage should be (metadata.LifecycleStage.Published)
      val Seq(previousIndex) = pgu.datasetMapReader.indexes(previousCopy)
      val imPreviouslyPublished = new IndexManager(pgu, previousCopy)
      val previousDbIndexes = jdbcIndexes(pgu.conn, previousCopy.dataTableName)
      val previousDbIndexName = imPreviouslyPublished.dbIndexName(previousIndex)
      previousDbIndexes.contains(previousDbIndexName) should be(true)
    }
  }

  private def copyColumns(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], fromCopy: TruthCopyInfo, toCopy: TruthCopyInfo, baseId: Int) {
    val columnIdMap = pgu.datasetMapReader.schema(fromCopy)
    columnIdMap.values.zipWithIndex.foreach { case (colInfo, idx) =>
      val toCol = com.socrata.datacoordinator.secondary.ColumnInfo[SoQLType](
        new ColumnId(baseId + idx), colInfo.userColumnId, colInfo.fieldName, colInfo.typ,
        colInfo.isSystemPrimaryKey, colInfo.isUserPrimaryKey, colInfo.isVersion, None
      )
      ColumnCreatedHandler(pgu, toCopy, toCol)
    }
  }
}
