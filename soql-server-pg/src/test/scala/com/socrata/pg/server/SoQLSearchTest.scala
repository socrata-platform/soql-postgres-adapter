package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLSearchTest extends SoQLTest {


  test("one token") {
    compareSoqlResult("select name, make, color search 'blue' order by name", "search-1-token.json")
  }

  test("two tokens") {
    compareSoqlResult("select name, make, color search 'blue jade' order by name", "search-2-tokens.json")
  }

  test("search and where") {
    compareSoqlResult("select name, make, color where make = 'APCO' search 'blue' order by name", "search-and-where.json")
  }

  test("search and where ci") {
    compareSoqlResult("select name, make, color where make = 'apco' search 'blue' order by name", "search-and-where.json", caseSensitivity = CaseInsensitive)
  }
}
