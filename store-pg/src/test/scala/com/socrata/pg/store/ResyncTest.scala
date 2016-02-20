package com.socrata.pg.store

import scala.language.reflectiveCalls
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.joda.time.DateTime


class ResyncTest extends PGSecondaryTestBase with PGStoreTestBase with PGSecondaryUniverseTestBase {

  test("handle resync") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo("monkey", "locale", "obfuscate".getBytes)
      val secondaryCopyInfo = CopyInfo(new CopyId(123), 1, LifecycleStage.Published, 55, new DateTime())
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
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator), Seq.empty)

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
      pgs.doResync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator), Seq.empty)

      // most basic validation... we have 3 rows
      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 3

    }
  }


}
