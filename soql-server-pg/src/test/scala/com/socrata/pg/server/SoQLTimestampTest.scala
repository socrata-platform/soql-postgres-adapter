package com.socrata.pg.server

class SoQLTimestampTest extends SoQLTest {

  test("date_trunc at time zone") {
    compareSoqlResult(
      """select date_trunc_ym(certified, 'PDT') as g, count(*) group by g order by g""",
      "group-fixedtimestamp-trunc-ym-time-zone.json")
  }

  test("floating_timestamp date_diff_d") {
    compareSoqlResult(
      """select name, date_diff_d(to_floating_timestamp(certified, 'EST'), available) - 149 as d1, date_diff_d('2008-08-01T18:12:34'::floating_timestamp, available) as d2 where name='Karma'""",
      "where-date-diff-d.json")
  }

  test("fixed_timestamp date_diff_d") {
    compareSoqlResult(
      """select name, date_diff_d('2008-03-02T12:11:22-05'::fixed_timestamp, certified) as d1, date_diff_d('2008-03-01T15:11:22-05'::fixed_timestamp, certified) as d2 where name='Karma'""",
      "where-date-diff-d.json")
  }
}
