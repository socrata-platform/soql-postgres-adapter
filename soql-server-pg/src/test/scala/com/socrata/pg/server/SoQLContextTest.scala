package com.socrata.pg.server


class SoQLContextTest extends SoQLTest {
  test("get_context(c)") {
    compareSoqlResult("select count(*) where make = get_context('make')", "where-get-context.json",
                      context = Map("make" -> "Skywalk",
                                    "model" -> "X-1500"))
  }

  test("get_context(nonexistant)") {
    compareSoqlResult("select get_context('does not exist') as ctx limit 1", "context-nonexistant.json",
                      context = Map("make" -> "Skywalk",
                                    "model" -> "X-1500"))
  }

  test("get_context(null)") {
    compareSoqlResult("select get_context(null) as ctx limit 1", "context-nonexistant.json")
  }
}
