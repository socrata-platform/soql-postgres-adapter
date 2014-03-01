package com.socrata.pg.server

import com.socrata.pg.store.PGSecondaryTestBase

class SoQLConversionFunctionsTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  test("create new select text, number") {
    compareSoqlResult("select make, name, v_max::text where make = 'APCO' order by name", "where-conv.json")
  }
}
