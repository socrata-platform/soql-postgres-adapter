package com.socrata.pg.store

import scala.language.reflectiveCalls
import com.socrata.datacoordinator.secondary.{ColumnInfo, LifecycleStage, CopyInfo, DatasetInfo}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.rojoma.simplearm.util._
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.joda.time.DateTime


class ResyncTest extends PGSecondaryTestBase with PGStoreTestBase with PGSecondaryUniverseTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle resync") {
    withPgu() { pgu =>
      val pgs = new PGSecondary(config)
      val secondaryDatasetInfo = DatasetInfo("monkey", "locale", "obfuscate".getBytes)
      val secondaryCopyInfo = CopyInfo(new CopyId(123), 1, LifecycleStage.Published, 55, new DateTime())
      val cookie = Option("monkey")

      val newSchema = ColumnIdMap[ColumnInfo[SoQLType]](
        new ColumnId(10) -> ColumnInfo[SoQLType](new ColumnId(10), new UserColumnId(":id"), SoQLID, true, false, false),
        new ColumnId(11) -> ColumnInfo[SoQLType](new ColumnId(11), new UserColumnId(":version"), SoQLVersion, false, false, true),
        new ColumnId(12) -> ColumnInfo[SoQLType](new ColumnId(12), new UserColumnId("my column 11"), SoQLText, false, false, false)
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
      pgs._resync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows.iterator))

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
      pgs._resync(pgu, secondaryDatasetInfo, secondaryCopyInfo, newSchema, cookie, unmanaged(rows2.iterator))

      // most basic validation... we have 3 rows
      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, secondaryDatasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.size shouldEqual 3

    }
  }


}
