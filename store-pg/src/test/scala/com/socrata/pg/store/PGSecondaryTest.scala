package com.socrata.pg.store

import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id.{RowId, UserColumnId, ColumnId, CopyId}
import com.socrata.soql.types._
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap

/**
 * Test... the PGSecondary.
 */
class PGSecondaryTest extends PGSecondaryTestBase {
  import PGSecondaryUtil._


  test("handle ColumnCreated") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 2L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId("mysystemidcolumn"), SoQLText, true, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId("myuseridcolumn"), SoQLText, false, true, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("myversioncolumn"), SoQLText, false, false, true)),
        ColumnCreated(ColumnInfo(new ColumnId(9127), new UserColumnId("mycolumn"), SoQLText, false, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9128), new UserColumnId(":updated_at"), SoQLTime, false, false, false))
      ).iterator
      pgs._version(datasetInfo, dataVersion+1, None, events, conn)
    }
  }

  test("handle SystemRowIdentifierChanged") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 2L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false))
      ).iterator
      pgs._version(datasetInfo, dataVersion+1, None, events, conn)
    }
  }


  test("handle row insert") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 2L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)

      val row1 = ColumnIdMap() + (new ColumnId(9124), new SoQLID(1000)) + (new ColumnId(9126), new SoQLText("foo"))
      val row2 = ColumnIdMap() + (new ColumnId(9124), new SoQLID(1001)) + (new ColumnId(9126), new SoQLText("foo2"))

      val events = Seq(
        WorkingCopyCreated(copyInfo),
        ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, true, false, false)),
        ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId(":version"), SoQLID, false, false, true)),
        ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("mycolumn"), SoQLText, false, false, false)),
        SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
        RowDataUpdated(Seq(Insert(new RowId(1000), row1), Insert(new RowId(1001), row2)))

      ).iterator
      pgs._version(datasetInfo, dataVersion+1, None, events, conn)
    }
  }
}
