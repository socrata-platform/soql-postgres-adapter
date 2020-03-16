package com.socrata.pg.soql

import SqlizerTest._

// scalastyle:off null
class SqlizerJsonbTest extends SqlizerTest {

  // test("document jsonb field optimized eq query should use operator @>") {
  //   val soql = "SELECT document WHERE document.filename = 'document.txt'"
  //   val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
  //   sql should be ("""SELECT t1.document FROM t1 WHERE (t1.document @> ('{"filename":"' || ? || '"}')::jsonb)""")
  //   val params = setParams.map { (setParam) => setParam(None, 0).get }
  //   params should be (Seq("document.txt"))
  // }

  // test("document jsonb field non-optimized eq query should use operator ->>") {
  //   val soql = "SELECT document WHERE document.file_id = case_number"
  //   val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
  //   sql should be ("SELECT t1.document FROM t1 WHERE ((t1.document->>'file_id') = t1.case_number)")
  //   setParams.isEmpty should be (true)
  // }
}
