package com.socrata.pg.server


class SoQLLocationTest extends SoQLTest {
  test("select location") {
    compareSoqlResult("""select code, location where code='LOCATION'""", "select-location.json")
  }
}
