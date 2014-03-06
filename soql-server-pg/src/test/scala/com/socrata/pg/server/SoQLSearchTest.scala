package com.socrata.pg.server

class SoQLSearchTest extends SoQLTest {


  test("one token") {
    compareSoqlResult("select name, make, color search 'blue' order by name", "search-1-token.json")
  }

  test("two tokens") {
    compareSoqlResult("select name, make, color search 'blue jade' order by name", "search-2-tokens.json")
  }

  test("search and where") {
    compareSoqlResult("select name, make, color where make = 'apco' search 'blue' order by name", "search-and-where.json")
  }
}
