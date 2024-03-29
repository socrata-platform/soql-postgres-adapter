package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerUrlTest extends SqlizerTest {

  test("url subcolumn") {
    val soql = "SELECT url.url as url_url WHERE url.description = 'Home Site' order by url_url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ("t1".url_url) FROM t1 WHERE (("t1".url_description) = ?) ORDER BY ("t1".url_url) nulls last""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("Home Site"))
  }

  test("url ctor") {
    val soql = "SELECT url('http://www.socrata.com', 'Home Site')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (e'[[http://www.socrata.com]]'),(e'[[Home Site]]') FROM t1")
    setParams.length should be (0)
  }

  test("url group") {
    val soql = "SELECT url, count(*) GROUP BY url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".url_url,"t1".url_description,(count(*)::numeric) FROM t1 GROUP BY "t1".url_url,"t1".url_description""")
    setParams should be (Seq.empty)
  }

  test("url order") {
    val soql = "SELECT url ORDER BY url"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".url_url,"t1".url_description FROM t1 ORDER BY "t1".url_url nulls last,"t1".url_description nulls last""")
    setParams should be (Seq.empty)
  }

  test("url count") {
    val soql = "SELECT count(url)"
    val expected = """SELECT ((count((coalesce(("t1".url_url),("t1".url_description)))))::numeric) FROM t1"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be(expected)
    setParams should be(Seq.empty)
  }

  test("url.url count") {
    val soql = "SELECT count(url.url)"
    val expected = """SELECT ((count(("t1".url_url)))::numeric) FROM t1"""
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be(expected)
    setParams should be(Seq.empty)
  }
}
