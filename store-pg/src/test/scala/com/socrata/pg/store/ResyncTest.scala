package com.socrata.pg.store

import scala.language.reflectiveCalls
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.joda.time.DateTime


class ResyncTest extends PGSecondaryTestBase with PGStoreTestBase with PGSecondaryUniverseTestBase {

  test("handle resync") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo(PGSecondaryUtil.testInternalName, "locale", "obfuscate".getBytes, None)
      val secondaryCopyInfo = CopyInfo(new CopyId(123), 1, LifecycleStage.Published, 55, 55, new DateTime())
      val cookie = Option("monkey")

      val newSchema = ColumnIdMap[ColumnInfo[SoQLType]](
        new ColumnId(10) -> ColumnInfo[SoQLType](new ColumnId(10), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None),
        new ColumnId(11) -> ColumnInfo[SoQLType](new ColumnId(11), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None),
        new ColumnId(12) -> ColumnInfo[SoQLType](new ColumnId(12), new UserColumnId("my column 11"), Some(ColumnName("my_column_11")), SoQLText, false, false, false, None)
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

      // first we resync a datset that doesn't exist
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator), Seq.empty, Seq.empty)

      // most basic validation... we have 2 rows
      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 2


      val rows2 = rows :+
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(12),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("taz"))

      // now we resync again with an extra row
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator), Seq.empty, Seq.empty)

      // most basic validation... we have 3 rows
      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 3

    }
  }

  test("handle drop copy") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo(PGSecondaryUtil.testInternalName, "locale", "obfuscate".getBytes, None)

      val snapshottedCopy = CopyInfo(new CopyId(123), 1, LifecycleStage.Snapshotted, 24, 24, new DateTime())
      val publishedCopy = CopyInfo(new CopyId(123), 2, LifecycleStage.Published, 55, 55, new DateTime())
      val unpublishedCopy = CopyInfo(new CopyId(123), 3, LifecycleStage.Unpublished, 57, 57, new DateTime())
      val discardedCopy = CopyInfo(new CopyId(123), 3, LifecycleStage.Discarded, 58, 58, new DateTime())

      val cookie = Option("monkey")

      val newSchema = ColumnIdMap[ColumnInfo[SoQLType]](
        new ColumnId(10) -> ColumnInfo[SoQLType](new ColumnId(10), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None),
        new ColumnId(11) -> ColumnInfo[SoQLType](new ColumnId(11), new UserColumnId(":version"), Some(ColumnName(":version")), SoQLVersion, false, false, true, None),
        new ColumnId(12) -> ColumnInfo[SoQLType](new ColumnId(12), new UserColumnId("my column 11"), Some(ColumnName("my_column_11")), SoQLText, false, false, false, None)
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

      val rows2 = rows :+
        ColumnIdMap[SoQLValue](
          new ColumnId(10) -> new SoQLID(12),
          new ColumnId(11) -> new SoQLVersion(20),
          new ColumnId(12) -> new SoQLText("taz"))

      // resync with the unpublished copy
      pgs.doDropCopy(pgu, secondaryDatasetInfo, snapshottedCopy, isLatestCopy = false)
      pgs.doResync(pgu, secondaryDatasetInfo, publishedCopy, newSchema, cookie, unmanaged(rows.iterator), Seq.empty, Seq.empty)
      pgs.doResync(pgu, secondaryDatasetInfo, unpublishedCopy, newSchema, cookie, unmanaged(rows2.iterator), Seq.empty, Seq.empty)

      // unpublished copy should be the "latest"
      for {
        unpublished <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
      } unpublished.copyNumber shouldEqual 3

      for {
        copies <- unmanaged(getTruthCopies(pgu, secondaryDatasetInfo))
      } {
        val copiesArray = copies.toArray

        // published copy
        val published = copiesArray(0)
        published.copyNumber shouldEqual 2
        published.dataVersion shouldEqual 55
        for {
          reader <- pgu.datasetReader.openDataset(published)
          rows <- reader.rows()
        } rows.size shouldEqual 2

        // unpublished copy
        val unpublished = copiesArray(1)
        unpublished.copyNumber shouldEqual 3
        unpublished.dataVersion shouldEqual 57
      }

      // now lets go through the resync process again, but this time the latest copy is discarded
      pgs.doDropCopy(pgu, secondaryDatasetInfo, snapshottedCopy, isLatestCopy = false)
      pgs.doResync(pgu, secondaryDatasetInfo, publishedCopy, newSchema, cookie, unmanaged(rows.iterator), Seq.empty, Seq.empty)
      pgs.doDropCopy(pgu, secondaryDatasetInfo, discardedCopy, isLatestCopy = true)

      // "latest" copy should be the published one
      for {
        published <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
      } published.copyNumber shouldEqual 2

      for {
        copies <- unmanaged(getTruthCopies(pgu, secondaryDatasetInfo))
      } {
        val copiesArray = copies.toArray

        // published copy
        val published = copiesArray(0)
        published.copyNumber shouldEqual 2
        published.dataVersion shouldEqual 55
        for {
          reader <- pgu.datasetReader.openDataset(published)
          rows <- reader.rows()
        } rows.size shouldEqual 2

        // discarded copy
        val discarded = copiesArray(1)
        discarded.copyNumber shouldEqual 3
        discarded.dataVersion shouldEqual 58
      }
    }
  }

}
