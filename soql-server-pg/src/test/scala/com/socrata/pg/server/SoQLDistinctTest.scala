package com.socrata.pg.server

class SoQLDistinctTest extends SoQLTest {
  test("distinct") {
    compareSoqlResult("select distinct make order by make", "distinct.json")
  }

  test("distinct with grouping") {
    compareSoqlResult("select distinct make, count(name) group by make order by make", "distinct-group.json")
  }

  test("count distinct") {
    compareSoqlResult("select count(distinct size)", "count-distinct.json")
  }

  test("count distinct with grouping") {
    compareSoqlResult("select make, count(distinct size), count(name) where make in ('APCO', 'OZONE') group by make order by make", "count-distinct-with-group.json")
  }
}
