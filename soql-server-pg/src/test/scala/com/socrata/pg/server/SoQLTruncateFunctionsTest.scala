package com.socrata.pg.server

class SoQLTruncateFunctionsTest extends SoQLTest {

  test("floating timestamp truncate - day") {
    compareSoqlResult("select date_trunc_ymd(available), count(*) where available is not null group by date_trunc_ymd(available) order by date_trunc_ymd(available)", "group-floatingtimestamp-trunc-ymd.json")
  }

  test("floating timestamp truncate - month") {
    compareSoqlResult("select date_trunc_ym(available), count(*) group by date_trunc_ym(available) order by date_trunc_ym(available) limit 5", "group-floatingtimestamp-trunc-ym.json", Some(9))
  }

  test("floating timestamp truncate - year") {
    compareSoqlResult("select date_trunc_y(available), count(*) group by date_trunc_y(available) order by date_trunc_y(available)", "group-floatingtimestamp-trunc-y.json")
  }
}