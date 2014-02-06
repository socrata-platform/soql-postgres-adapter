package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.secondary.Insert
import com.socrata.datacoordinator.secondary.RowDataUpdated
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.PGSecondaryTestBase
import com.socrata.soql.types.{SoQLText, SoQLID}
import scala.language.reflectiveCalls

class RowDataUpdatedHandlerTest extends PGSecondaryTestBase {

  import com.socrata.pg.store.PGSecondaryUtil._

  test("handle row insert") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      val row1 = ColumnIdMap() + (new ColumnId(9124), new SoQLID(1000)) + (new ColumnId(9126), new SoQLText("foo"))
      val row2 = ColumnIdMap() + (new ColumnId(9124), new SoQLID(1001)) + (new ColumnId(9126), new SoQLText("foo2"))

      val events = f.events ++ Seq(
        RowDataUpdated(Seq(Insert(new RowId(1000), row1), Insert(new RowId(1001), row2)))
      )
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, events.iterator)
    }
  }

}
