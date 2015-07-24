package com.socrata.pg.store.events

import scala.language.reflectiveCalls

import com.rojoma.simplearm.util.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.secondary.{Insert, RowDataUpdated, Truncated}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverseTestBase, PGStoreTestBase}
import com.socrata.soql.types.{SoQLID, SoQLText, SoQLVersion}
import com.typesafe.scalalogging.slf4j.Logging

class TruncateHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase with Logging {

  private val insertOps = Seq(
    (1000, 110, "foo"),
    (1001, 112, "foo2"),
    (1002, 114, "foo3")).map { r =>
    Insert(new RowId(r._1), ColumnIdMap()
      + ((new ColumnId(9124), new SoQLID(r._1)))
      + ((new ColumnId(9125), new SoQLVersion(r._2)))
      + ((new ColumnId(9126), new SoQLText(r._3)))
    )
  }

  test("truncate") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture
      val events = f.events ++ Seq(RowDataUpdated(insertOps), Truncated)

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)

      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.isEmpty should be (true)
    }
  }
}
