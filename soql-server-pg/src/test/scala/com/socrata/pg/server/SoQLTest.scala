package com.socrata.pg.server

import com.socrata.pg.store.PGSecondaryTestBase
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.query.PGQueryTestBase


abstract class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  override def afterAll = {
    withPgu() { pgu =>
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val tableName = pgu.datasetMapReader.latest(datasetInfo).dataTableName
      dropDataset(pgu, truthDatasetId)
      cleanupDroppedTables(pgu)
      hasDataTables(pgu.conn, tableName, datasetInfo) should be (false)
    }
    super.afterAll
  }
}
