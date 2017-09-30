package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLWindowFunctionsTest extends SoQLTest {

  test("avg over make") {
    compareSoqlResult("select code, make, avg(v_max) over(make) where v_max is not null order by code", "avg_over_columns.json")
  }

  test("avg over all rows") {
    compareSoqlResult("select code, make, avg(v_max) over() where v_max is not null order by code", "avg_over_all.json")
  }
}
