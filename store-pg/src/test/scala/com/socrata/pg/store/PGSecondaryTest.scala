package com.socrata.pg.store

import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.soql.types._
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo

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
      pgs._version(datasetInfo, dataVersion, None, events, conn)
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
      pgs._version(datasetInfo, dataVersion, None, events, conn)
    }
  }
}
