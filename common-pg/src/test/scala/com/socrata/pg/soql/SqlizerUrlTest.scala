package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerUrlTest extends SqlizerTest {

  test("url subcolumn") {
    val soql = "SELECT url.url as url_url WHERE url.description = 'Home Site' order by url_url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (url_url) FROM t1 WHERE ((url_description) = ?) ORDER BY (url_url) nulls last")
    println(sql)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("Home Site"))
  }

  test("url ctor") {
    val soql = "SELECT url('http://www.socrata.com', 'Home Site')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (?),(?) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("http://www.socrata.com", "Home Site"))
  }
}
