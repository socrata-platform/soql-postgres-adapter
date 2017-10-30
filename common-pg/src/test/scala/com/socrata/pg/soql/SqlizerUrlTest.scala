package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerUrlTest extends SqlizerTest {

  test("url subcolumn") {
    val soql = "SELECT url.url as url_url WHERE url.description = 'Home Site' order by url_url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (t1.url_url) FROM t1 WHERE ((t1.url_description) = ?) ORDER BY (t1.url_url) nulls last")
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

  test("url group") {
    val soql = "SELECT url, count(*) GROUP BY url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT t1.url_url,t1.url_description,(count(*)) FROM t1 GROUP BY t1.url_url,t1.url_description")
    setParams should be (Seq.empty)
  }

  test("url order") {
    val soql = "SELECT url ORDER BY url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT t1.url_url,t1.url_description FROM t1 ORDER BY t1.url_url nulls last,t1.url_description nulls last")
    setParams should be (Seq.empty)
  }
}
