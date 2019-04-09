package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLWindowFunctionsTest extends SoQLTest {

  test("avg over make") {
    compareSoqlResult("select code, make, avg(v_max) over(PARTITION BY make) where v_max is not null order by code", "avg_over_columns.json")
  }

  test("avg over all rows") {
    compareSoqlResult("select code, make, avg(v_max) over() where v_max is not null order by code", "avg_over_all.json")
  }

  test("one model from each make") {
    compareSoqlResult("""
        SELECT code, make, row_number() over(partition by make order by make, code) as rn
         WHERE make in ('APCO','OZONE','Skywalk') |>
               SELECT * WHERE rn <= 1 ORDER BY make""",
      "make_one.json")
  }
}
