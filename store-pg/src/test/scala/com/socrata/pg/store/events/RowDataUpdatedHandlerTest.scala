package com.socrata.pg.store.events

import scala.language.reflectiveCalls

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.secondary.{Delete, Insert, RowDataUpdated, Update}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store._
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logging

class RowDataUpdatedHandlerTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase with Logging {

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

  test("handle row insert") {
    withPgu() {
      pgu =>
        val f = columnsCreatedFixture
        val events = f.events ++ Seq(
          RowDataUpdated(insertOps)
        )

        f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)

        for {
          truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
          reader <- pgu.datasetReader.openDataset(truthCopyInfo)
          rows <- reader.rows()
        } rows.map(_(new ColumnId(9126))).collect { case SoQLText(s) => s }.toSet should contain theSameElementsAs Set("foo", "foo2", "foo3")
    }
  }

  test("handle row update") {
    withDb() {
      conn =>

        val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
        val f = columnsCreatedFixture
        val events = f.events :+ RowDataUpdated(insertOps)

        f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)

        val updateOps = Seq(
          (1000, 110, "bar"),
          (1002, 114, "bar3")).map { r =>
          Update(new RowId(r._1), ColumnIdMap()
            + ((new ColumnId(9124), new SoQLID(r._1)))
            + ((new ColumnId(9125), new SoQLVersion(r._2)))
            + ((new ColumnId(9126), new SoQLText(r._3)))
          )(None)
        }

        val updateEvents = Seq(RowDataUpdated(updateOps))

        f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 2, None, updateEvents.iterator)

        for {
          truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
          reader <- pgu.datasetReader.openDataset(truthCopyInfo)
          rows <- reader.rows()
        } rows.map(_(new ColumnId(9126))).collect { case SoQLText(s) => s }.toSet should contain theSameElementsAs Set("bar", "foo2", "bar3")
    }
  }

  test("handle row delete") {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
      val f = columnsCreatedFixture
      val events = f.events :+ RowDataUpdated(insertOps)

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)

      val deleteOps = Seq(
        1000,
        1002).map { r =>
        Delete(new RowId(r))(None)
      }

      val deleteEvents = Seq(RowDataUpdated(deleteOps))

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 2, None, deleteEvents.iterator)

      for {
        truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
        reader <- pgu.datasetReader.openDataset(truthCopyInfo)
        rows <- reader.rows()
      } rows.map(_(new ColumnId(9126))).collect { case SoQLText(s) => s }.toSet should contain theSameElementsAs Set("foo2")

    }
  }

}
