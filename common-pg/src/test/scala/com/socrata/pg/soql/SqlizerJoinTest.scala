package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerJoinTest  extends SqlizerTest {

  test("join wo alias") {
    val soql = "select case_number, primary_type, @type.description join @type on primary_type = @type.primary_type"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT t1.case_number,t1.primary_type,t2.description FROM t1 JOIN t2 ON (t1.primary_type = t2.primary_type)")
    setParams.length should be (0)
  }

  test("join with alias") {
    val soql = "select case_number, primary_type, @y.avg_temperature join @year as y on @y.year = year"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT t1.case_number,t1.primary_type,t3.avg_temperature FROM t1 JOIN t3 ON (t3.year = t1.year)")
    setParams.length should be (0)
  }

  test("join and chain") {
    val soql = "select case_number, primary_type, @y.avg_temperature join @year as y on @y.year = year |> select count(*)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(*)) FROM (SELECT t1.case_number as \"case_number\",t1.primary_type as \"primary_type\",t3.avg_temperature as \"avg_temperature\" FROM t1 JOIN t3 ON (t3.year = t1.year)) AS x1")
    setParams.length should be (0)
  }
}
