package com.socrata.pg.server

class SoQLTimestampTest extends SoQLTest {

  test("date_trunc at time zone") {
    compareSoqlResult(
      """select date_trunc_ym(certified, 'PDT') as g, count(*) group by g order by g""",
      "group-fixedtimestamp-trunc-ym-time-zone.json")
  }
}
