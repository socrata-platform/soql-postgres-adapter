package com.socrata.pg.server

/**
 * http://beta.dev.socrata.com/docs/datatypes/boolean.html
 */
class SoQLBoolFunctionsTest extends SoQLTest {

  test("and") {
    compareSoqlResult("select make, name, three_liner where three_liner and v_max > 50 order by name", "where-bool-and.json")
  }

  test("or") {
    compareSoqlResult("select make, name where make = 'OZONE' or make = 'GIN' order by name", "where-bool-or.json")
  }

  test("not") {
    compareSoqlResult("select make, name, three_liner where not three_liner order by name", "where-bool-not.json")
  }
}
