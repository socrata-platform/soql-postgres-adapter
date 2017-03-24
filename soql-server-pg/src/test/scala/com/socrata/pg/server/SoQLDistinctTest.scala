package com.socrata.pg.server

class SoQLDistinctTest extends SoQLTest {
  test("distinct") {
    compareSoqlResult("select distinct make order by make", "distinct.json")
  }

  test("distinct with grouping") {
    compareSoqlResult("select distinct make, count(name) group by make order by make", "distinct-group.json")
  }
}
