package com.socrata.pg.store.events

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.secondary.{Update, Insert, RowDataUpdated}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse, PGSecondaryTestBase}
import com.socrata.soql.types._
import scala.language.reflectiveCalls
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.typesafe.scalalogging.slf4j.Logging
import com.socrata.datacoordinator.secondary.RowDataUpdated
import com.socrata.datacoordinator.secondary.Update
import com.socrata.datacoordinator.secondary.Insert


class RowDataUpdatedHandlerTest extends PGSecondaryTestBase with Logging {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle row insert") {
    withPgu() {
      pgu =>
        val f = columnsCreatedFixture

        val row1 = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1000)) +(new ColumnId(9126), new SoQLText("foo"))
        val row2 = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1001)) +(new ColumnId(9126), new SoQLText("foo2"))

        val events = f.events ++ Seq(
          RowDataUpdated(Seq(Insert(new RowId(1000), row1), Insert(new RowId(1001), row2)))
        )
        f.pgs._version(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)


        val rowsSet = for {
          truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
          reader <- pgu.datasetReader.openDataset(truthCopyInfo)
          rows <- reader.rows()
        } yield {
          rows.map { cidMap =>
            cidMap(new ColumnId(9126)) match {
              case SoQLText(s) => s
              case _ => throw new RuntimeException("unexpected column value")
            }
          }.toSet
        }

        val expectedRowsSet = Set("foo", "foo2")
        assert(rowsSet equals expectedRowsSet, s"Should have found ${expectedRowsSet}, but found ${rowsSet}")

    }
  }

  test("handle row update") {
    withDb() {
      conn =>
        val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)

        val f = columnsCreatedFixture

        val row1 = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1000)) +(new ColumnId(9125), new SoQLVersion(110)) +(new ColumnId(9126), new SoQLText("foo"))
        val row2 = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1001)) +(new ColumnId(9125), new SoQLVersion(112)) +(new ColumnId(9126), new SoQLText("foo2"))

        val events = f.events ++ Seq(
          RowDataUpdated(Seq(Insert(new RowId(1000), row1), Insert(new RowId(1001), row2)))
        )
        f.pgs._version(pgu, f.datasetInfo, f.dataVersion + 1, None, events.iterator)

        val row1u = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1000)) +(new ColumnId(9125), new SoQLVersion(110)) +(new ColumnId(9126), new SoQLText("bar"))
        val row2u = ColumnIdMap() +(new ColumnId(9124), new SoQLID(1001)) +(new ColumnId(9125), new SoQLVersion(112)) +(new ColumnId(9126), new SoQLText("bar2"))

        val updateEvents = Seq(
          RowDataUpdated(Seq(Update(new RowId(1000), row1u)(Option(row1)), Update(new RowId(1001), row2u)(Option(row2))))
        )

        f.pgs._version(pgu, f.datasetInfo, f.dataVersion + 2, None, updateEvents.iterator)


        val rowsSet = for {
          truthCopyInfo <- unmanaged(getTruthCopyInfo(pgu, f.datasetInfo))
          reader <- pgu.datasetReader.openDataset(truthCopyInfo)
          rows <- reader.rows()
        } yield {
          rows.map { cidMap =>
            cidMap(new ColumnId(9126)) match {
              case SoQLText(s) => s
              case _ => throw new RuntimeException("unexpected column value")
            }
          }.toSet
        }

        val expectedRowsSet = Set("bar", "bar2")
        assert(rowsSet equals expectedRowsSet, s"Should have found ${expectedRowsSet}, but found ${rowsSet}")

    }
  }

}
