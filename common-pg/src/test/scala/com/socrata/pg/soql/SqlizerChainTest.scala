package com.socrata.pg.soql

import SqlizerTest._

class SqlizerChainTest extends SqlizerTest {

  implicit val materialized = false

  def index = if (materialized) 1 else 0

  test("select literal and group by has aliases") {
    val soql = "SELECT 'one' as one, max('two') as two GROUP BY one |> SELECT /*hint*/ one, two"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT "one","two" FROM (SELECT e'one' as "one",(max(?)) as "two" FROM t1 GROUP BY 1) AS "x1"""",
      """WITH "x1" AS MATERIALIZED (SELECT e'one' as "one",(max(?)) as "two" FROM t1 GROUP BY 1)
        |SELECT "one","two" FROM x1""".stripMargin)
    sql should be (expected(index))
    val expectedParams = Seq(Seq("two"), Seq("two"))
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be (expectedParams(index))
  }

  test("chained soql set params positions match place holder positions in sql") {
    val soql =
      """SELECT 'aa' as aa WHERE primary_type !='00' |>
        |SELECT /*hint*/ aa || 'b' as bb WHERE aa !='11'""".stripMargin
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT ("aa" || ?) FROM (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?)) AS "x1" WHERE ("aa" != ?)""",
      """WITH "x1" AS MATERIALIZED (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?))
        |SELECT ("aa" || ?) FROM x1 WHERE ("aa" != ?)""".stripMargin
    )
    sql should be (expected(index))
    val expectedParams = Seq(Seq("b", "aa", "00", "11"), Seq("aa", "00", "b", "11"))
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(expectedParams(index))
  }

  test("chained (x3) soql set params positions match place holder positions in sql") {
    val soql =
      """SELECT 'aa' as aa WHERE primary_type !='00' |>
        |SELECT /*hint*/ aa || 'b' as bb WHERE aa !='11' |>
        |SELECT bb || 'c' as cc WHERE bb !='22'""".stripMargin
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT ("bb" || ?) FROM (SELECT ("aa" || ?) as "bb" FROM (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?)) AS "x1" WHERE ("aa" != ?)) AS "x1" WHERE ("bb" != ?)""",
      """SELECT ("bb" || ?) FROM (WITH "x1" AS MATERIALIZED (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?))
        |SELECT ("aa" || ?) as "bb" FROM x1 WHERE ("aa" != ?)) AS "x1" WHERE ("bb" != ?)""".stripMargin
    )
    sql should be (expected(index))
    val expectedParams = Seq(
      Seq("c", "b", "aa", "00", "11", "22"),
      Seq("c", "aa", "00", "b", "11", "22")
    )
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(expectedParams(index))
  }

  test("chained (x3) soql double materialized set params positions match place holder positions in sql") {
    val soql =
      """SELECT 'aa' as aa WHERE primary_type !='00' |>
        |SELECT /*hint*/ aa || 'b' as bb WHERE aa !='11' |>
        |SELECT /*hint*/ bb || 'c' as cc WHERE bb !='22'""".stripMargin
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT ("bb" || ?) FROM (SELECT ("aa" || ?) as "bb" FROM (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?)) AS "x1" WHERE ("aa" != ?)) AS "x1" WHERE ("bb" != ?)""",
      """WITH "x1" AS MATERIALIZED (WITH "x1" AS MATERIALIZED (SELECT ? as "aa" FROM t1 WHERE ("t1".primary_type != ?))
        |SELECT ("aa" || ?) as "bb" FROM x1 WHERE ("aa" != ?))
        |SELECT ("bb" || ?) FROM x1 WHERE ("bb" != ?)""".stripMargin
    )
    sql should be (expected(index))
    val expectedParams = Seq(
      Seq("c", "b", "aa", "00", "11", "22"),
      Seq("aa", "00", "b", "11", "c", "22")
    )
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(expectedParams(index))
  }

  test("chained soql only adds ST_AsBinary in the outermost sql") {
    val soql = "select point, object |> select /*hint*/ point where within_box(point, 1, 2, 3, 4)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT ST_AsBinary("point") AS point FROM (SELECT "t1".point as "point","t1".object as "object" FROM t1) AS "x1" WHERE (ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ "point")""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".point as "point","t1".object as "object" FROM t1)
        |SELECT ST_AsBinary("point") AS point FROM x1 WHERE (ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ "point")""".stripMargin
    )
    sql should be (expected(index))
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(Seq(2, 3, 4, 1).map(BigDecimal(_)))
  }

  test("chained soql only adds ST_AsBinary - location in the outermost sql") {
    val soql = "select location, object |> select /*hint*/ location::point where within_box(location::point, 1, 2, 3, 4)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = Seq(
      """SELECT ST_AsBinary(("location_geom")) AS location_point FROM (SELECT "t1".location_geom as "location_geom","t1".location_address as "location_address","t1".object as "object" FROM t1) AS "x1" WHERE (ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ ("location_geom"))""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".location_geom as "location_geom","t1".location_address as "location_address","t1".object as "object" FROM t1)
        |SELECT ST_AsBinary(("location_geom")) AS location_point FROM x1 WHERE (ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ ("location_geom"))""".stripMargin
    )
    sql should be (expected(index))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(2, 3, 4, 1).map(BigDecimal(_)))
  }

  test("chained search scope is limited to the previous result") {
    val soql = "select case_number, primary_type search 'oNe' |> select /*hint*/ * search 'tWo'"

    // search before select, filter, grouping, join
    val ParametricSql(Seq(sqlLeadingSearch), setParamsLeadingSearch) = sqlize(soql, CaseSensitive, useRepsWithId = true, leadingSearch = true)
    val expectedSqlLeadingSearch = Seq(
      """SELECT "case_number","primary_type" FROM (SELECT "t1".case_number_6 as "case_number","t1".primary_type_7 as "primary_type" FROM t1 WHERE (to_tsvector('english', coalesce("t1".array_12,'') || ' ' || coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".object_11,'') || ' ' || coalesce("t1".primary_type_7,'') || ' ' || coalesce("t1".url_21_description,'') || ' ' || coalesce("t1".url_21_url,'')) @@ plainto_tsquery('english', ?))) AS "x1" WHERE (to_tsvector('english', coalesce("case_number",'') || ' ' || coalesce("primary_type",'')) @@ plainto_tsquery('english', ?))""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".case_number_6 as "case_number","t1".primary_type_7 as "primary_type" FROM t1 WHERE (to_tsvector('english', coalesce("t1".array_12,'') || ' ' || coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".object_11,'') || ' ' || coalesce("t1".primary_type_7,'') || ' ' || coalesce("t1".url_21_description,'') || ' ' || coalesce("t1".url_21_url,'')) @@ plainto_tsquery('english', ?)))
        |SELECT "case_number","primary_type" FROM x1 WHERE (to_tsvector('english', coalesce("case_number",'') || ' ' || coalesce("primary_type",'')) @@ plainto_tsquery('english', ?))""".stripMargin)
    sqlLeadingSearch should be (expectedSqlLeadingSearch(index))
    val paramsLeadingSearch = setParamsLeadingSearch.map { setParam => setParam(None, 0).get }
    paramsLeadingSearch should be(Seq("oNe", "tWo"))

    // search after select, filter, grouping, join
    val expectedSqlTrailingSearch = Seq(
      """SELECT "case_number","primary_type" FROM (SELECT "t1".case_number_6 as "case_number","t1".primary_type_7 as "primary_type" FROM t1 WHERE (to_tsvector('english', coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".primary_type_7,'')) @@ plainto_tsquery('english', ?))) AS "x1" WHERE (to_tsvector('english', coalesce("case_number",'') || ' ' || coalesce("primary_type",'')) @@ plainto_tsquery('english', ?))""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".case_number_6 as "case_number","t1".primary_type_7 as "primary_type" FROM t1 WHERE (to_tsvector('english', coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".primary_type_7,'')) @@ plainto_tsquery('english', ?)))
        |SELECT "case_number","primary_type" FROM x1 WHERE (to_tsvector('english', coalesce("case_number",'') || ' ' || coalesce("primary_type",'')) @@ plainto_tsquery('english', ?))""".stripMargin
    )
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive, useRepsWithId = true, leadingSearch = false)
    sql should be (expectedSqlTrailingSearch(index))
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(Seq("oNe", "tWo"))
  }

  test("chained search scope is limited to the previous result and no searchable types is converted to false") {
    val soql = "select id search 'oNe' |> select /*hint*/ * search 'tWo'"

    // search before select, filter, grouping, join
    val ParametricSql(Seq(sqlLeadingSearch), setParamsLeadingSearch) = sqlize(soql, CaseSensitive, useRepsWithId = true, leadingSearch = true)
    val expectedSqlLeadingSearch = Seq(
      """SELECT "id" FROM (SELECT "t1".id_5 as "id" FROM t1 WHERE (to_tsvector('english', coalesce("t1".array_12,'') || ' ' || coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".object_11,'') || ' ' || coalesce("t1".primary_type_7,'') || ' ' || coalesce("t1".url_21_description,'') || ' ' || coalesce("t1".url_21_url,'')) @@ plainto_tsquery('english', ?))) AS "x1" WHERE (false)""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".id_5 as "id" FROM t1 WHERE (to_tsvector('english', coalesce("t1".array_12,'') || ' ' || coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".object_11,'') || ' ' || coalesce("t1".primary_type_7,'') || ' ' || coalesce("t1".url_21_description,'') || ' ' || coalesce("t1".url_21_url,'')) @@ plainto_tsquery('english', ?)))
        |SELECT "id" FROM x1 WHERE (false)""".stripMargin
    )
    sqlLeadingSearch should be (expectedSqlLeadingSearch(index))
    val paramsLeadingSearch = setParamsLeadingSearch.map { (setParam) => setParam(None, 0).get }
    paramsLeadingSearch should be(Seq("oNe"))

    // search after select, filter, grouping, join
    val expectedSqlTrailingSearch = Seq(
      """SELECT "id" FROM (SELECT "t1".id_5 as "id" FROM t1 WHERE (false)) AS "x1" WHERE (false)""",
      """WITH "x1" AS MATERIALIZED (SELECT "t1".id_5 as "id" FROM t1 WHERE (false))
        |SELECT "id" FROM x1 WHERE (false)""".stripMargin
    )
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive, useRepsWithId = true, leadingSearch = false)
    sql should be (expectedSqlTrailingSearch(index))
    setParams.length should be (0)
  }

  test("window function row_number") {
    val soql = "select row_number() over(partition by primary_type, year order by primary_type, year) as rn |> select /*hint*/ * where rn < 2"
    val expected = Seq(
      """SELECT "rn" FROM (SELECT (row_number() over( partition by "t1".primary_type,"t1".year order by "t1".primary_type nulls last,"t1".year nulls last)) as "rn" FROM t1) AS "x1" WHERE ("rn" < ?)""",
      """WITH "x1" AS MATERIALIZED (SELECT (row_number() over( partition by "t1".primary_type,"t1".year order by "t1".primary_type nulls last,"t1".year nulls last)) as "rn" FROM t1)
        |SELECT "rn" FROM x1 WHERE ("rn" < ?)""".stripMargin
    )
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { setParam => setParam(None, 0).get }
    sql should be (expected(index))
    params should be(Seq(2))
  }

  test("distinct on chain count") {
    val soql = "SELECT Distinct on (case_number, 'one') case_number |> SELECT count(*)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (count(*)::numeric) FROM (SELECT DISTINCT ON ("t1".case_number,?) "t1".case_number as "case_number" FROM t1) AS "x1"""")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("one" ))
  }
}
