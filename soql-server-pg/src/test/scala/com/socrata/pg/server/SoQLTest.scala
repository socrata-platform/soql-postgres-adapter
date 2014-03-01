package com.socrata.pg.server

import com.socrata.pg.store._

class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {

  override def beforeAll = {
    createDatabases()
    withDb() { conn =>
      importDataset(conn)
    }
  }

  test("create new select text, number") {
    compareSoqlResult("select make, name", "result.json")
  }

  test("make = 'APCO'") {
    compareSoqlResult("select make, name where make = 'apco' order by name", "where-eq.json")
  }

  test("starts_with(make, 'Skyw')") {
    compareSoqlResult("select make, name where starts_with(make, 'Skyw') order by name", "where-starts_with.json")
  }
}
