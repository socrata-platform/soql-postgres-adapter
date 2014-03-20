package com.socrata.pg.server

import com.rojoma.simplearm.util._
import com.socrata.pg.soql.CaseSensitivity
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse, PGTestSecondary, PGSecondaryTestBase}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import java.sql.PreparedStatement

abstract class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  override def afterAll = {
    dropDataset()
    super.afterAll
  }

  protected def dropDataset() = {
    withDb() { conn =>
      val secondary = new PGTestSecondary(config, TestDatabaseConfigKey)
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon )
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val tableName = pgu.datasetMapReader.latest(datasetInfo).dataTableName
      val dsName = s"$dcInstance.${truthDatasetId.underlying}"
      secondary.dropDataset(dsName, None)
      conn.commit()
      while (pgu.tableCleanup.cleanupPendingDrops()) { }
      pgu.commit()
      // Check that base table, audit table and log table are deleted.
      val tableExistSql = s"select * from pg_class where relname in ('$tableName', '${datasetInfo.auditTableName}', '${datasetInfo.logTableName}') and relkind='r'"
      using (conn.prepareStatement(tableExistSql)) { stmt: PreparedStatement =>
        stmt.executeQuery().next() should be (false)
      }
    }
  }
}
