package com.socrata.pg.server

import com.socrata.pg.store.PGSecondaryTestBase

abstract class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }
}
