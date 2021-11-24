package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLWindowFunctionsTest extends SoQLTest {

  test("avg over make") {
    Seq(("", "avg_over_columns.json"),
        (" FILTER (WHERE TRUE)", "avg_over_columns.json"),
        (" FILTER (WHERE FALSE)", "avg_over_columns_nulls.json")).foreach { case (filter, expectedFile) =>
      val query = s"SELECT code, make, avg(v_max)${filter} OVER(PARTITION BY make) as avg_v_max_over_partition_by_make WHERE v_max is not null ORDER BY code"
      compareSoqlResult(query, expectedFile)
    }
  }

  test("avg over all rows") {
    compareSoqlResult("select code, make, avg(v_max) over() where v_max is not null order by code", "avg_over_all.json")
  }

  test("avg over all rows with unreserved keyword - range and null\"s\" first") {
    compareSoqlResult("select code, make, avg(v_max) over() as range where v_max is not null order by code nulls first", "avg_over_all_unreserved_keyword.json")
  }

  test("one model from each make") {
    compareSoqlResult("""
        SELECT code, make, row_number() over(partition by make order by make, code) as rn
         WHERE make in ('APCO','OZONE','Skywalk') |>
               SELECT * WHERE rn <= 1 ORDER BY make""",
      "make_one.json")
  }
}
