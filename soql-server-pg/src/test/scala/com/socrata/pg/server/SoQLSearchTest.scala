package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLSearchTest extends SoQLTest {

  test("one token") {
    compareSoqlResult("select name, make, color search 'blue' order by name", "search-1-token.json")
  }

  test("prev query has where and this query has search only") {
    compareSoqlResult("select * where true |> select name, make, color search 'blue' order by name", "search-1-token.json")
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

  test("number search") {
    compareSoqlResult("select name, make, aspect_ratio search '4.9' order by name", "search-number.json", leadingSearch = false)
    compareSoqlResult("select name, make search '4.9' order by name", "empty.json", leadingSearch = false)
    compareSoqlResult("select name, make search '4.9' order by name", "search-number-leading.json", leadingSearch = true)
  }

  test("grouping and number search") {
    compareSoqlResult("select make, count(*) as count group by make search '2' order by make", "search-post-group-number.json", leadingSearch = false)
    compareSoqlResult("select make, count(*) as count group by make search '2' order by make", "empty.json", leadingSearch = true)
  }
}
