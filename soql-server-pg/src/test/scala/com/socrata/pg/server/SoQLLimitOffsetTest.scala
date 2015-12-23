package com.socrata.pg.server

class SoQLLimitOffsetTest  extends SoQLTest {

  test("plain with row count") {
    compareSoqlResult("select make, name order by name offset 2 limit 5", "limitoffset-plain.json", Some(16))
  }

  test("plain w/o row count") {
    compareSoqlResult("select make, name order by name offset 2 limit 5", "limitoffset-plain.json", None)
  }

  test("group by with row count") {
    compareSoqlResult("select make, count(name) where not starts_with(make, 'z') group by make having count(name) > 0 order by make limit 1 offset 5", "limitoffset-group.json", Some(8))
  }

  test("group by w/o row count") {
    compareSoqlResult("select make, count(name) where not starts_with(make, 'z') group by make having count(name) > 0 order by make limit 1 offset 5", "limitoffset-group.json", None)
  }

  test("search with row count") {
    compareSoqlResult("select name, make search 'apco' order by name limit 2 offset 1", "limitoffset-search.json", Some(4))
  }

  test("search w/o row count") {
    compareSoqlResult("select name, make search 'apco' order by name limit 2 offset 1", "limitoffset-search.json", None)
  }
}
