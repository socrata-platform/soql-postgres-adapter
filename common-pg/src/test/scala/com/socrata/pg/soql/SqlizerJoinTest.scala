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
    sql should be ("""SELECT t1.case_number,t1.primary_type,_y.avg_temperature FROM t1 JOIN t3 as _y ON (_y.year = t1.year)""")
    setParams.length should be (0)
  }

  test("join and chain") {
    val soql = "select case_number, primary_type, @y.avg_temperature join @year as y on @y.year = year |> select count(*)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (count(*)::numeric) FROM (SELECT t1.case_number as "case_number",t1.primary_type as "primary_type",_y.avg_temperature as "avg_temperature" FROM t1 JOIN t3 as _y ON (_y.year = t1.year)) AS x1""")
    setParams.length should be (0)
  }

  test("group and then join with qualified fields from join") {
    val soql = "select primary_type, count(case_number), max(year) as year group by primary_type |> select primary_type, year, @y.avg_temperature join @year as y on @y.year = year and year = 2000"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT x1."primary_type",x1."year",_y.avg_temperature FROM (SELECT t1.primary_type as "primary_type",((count(t1.case_number))::numeric) as "count_case_number",(max(t1.year)) as "year" FROM t1 GROUP BY t1.primary_type) AS x1 JOIN t3 as _y ON ((_y.year = x1."year") and (x1."year" = ?))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(2000))
  }

  test("join with sub-query - select *") {
    val soql = "select case_number, primary_type, @y.year join (SELECT year FROM @year) as y on @y.year = year"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT t1.case_number,t1.primary_type,_y."year" FROM t1 JOIN (SELECT t3.year as "year" FROM t3) as _y ON (_y."year" = t1.year)""")
    setParams.length should be (0)
  }

  test("join with sub-query - select columns") {
    val soql = "select case_number, primary_type, @y.year, @y.avg_temperature join (SELECT * FROM @year) as y on @y.year = year"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT t1.case_number,t1.primary_type,_y."year",_y."avg_temperature" FROM t1 JOIN (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) as _y ON (_y."year" = t1.year)""")
    setParams.length should be (0)
  }

  test("join with chained sub-query - select columns") {
    val soql = "select case_number, primary_type, @y.year, @y.avg_temperature join (SELECT * FROM @year |> SELECT year, avg_temperature WHERE avg_temperature > 30) as y on @y.year = year and year = 2000"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT t1.case_number,t1.primary_type,_y."year",_y."avg_temperature" FROM t1 JOIN (SELECT "year" as "year","avg_temperature" as "avg_temperature" FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1 WHERE ("avg_temperature" > ?)) as _y ON ((_y."year" = t1.year) and (t1.year = ?))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(30, 2000).map(BigDecimal(_)))
  }

  test("join and date extract") {
    val soql = "select case_number, primary_type, date_extract_woy(@type.registered) join @type on primary_type = @type.primary_type"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT t1.case_number,t1.primary_type,(extract(week from t2.registered)::numeric) FROM t1 JOIN t2 ON (t1.primary_type = t2.primary_type)")
    setParams.length should be (0)
  }

  test("chained primary table join with chained sub-query - select columns") {
    val soql = "select case_number, primary_type |> select primary_type, count(*) as total group by primary_type |> select primary_type, total |>  select primary_type, total, @y.year, @y.avg_temperature join (SELECT * FROM @year |> SELECT year, avg_temperature WHERE avg_temperature > 30) as y on @y.year = total and total = 2000"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT x1."primary_type",x1."total",_y."year",_y."avg_temperature" FROM (SELECT "primary_type" as "primary_type","total" as "total" FROM (SELECT "primary_type" as "primary_type",(count(*)::numeric) as "total" FROM (SELECT t1.case_number as "case_number",t1.primary_type as "primary_type" FROM t1) AS x1 GROUP BY "primary_type") AS x1) AS x1 JOIN (SELECT x1."year" as "year",x1."avg_temperature" as "avg_temperature" FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1 WHERE (x1."avg_temperature" > ?)) as _y ON ((_y."year" = x1."total") and (x1."total" = ?))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(30, 2000).map(BigDecimal(_)))

    val soqlChainSelectMax = soql + " |> select max(year)"
    val ParametricSql(Seq(sqlChainSelectMax), setParamsChainSelectMax) = sqlize(soqlChainSelectMax, CaseSensitive)
    sqlChainSelectMax should be ("""SELECT (max("year")) FROM (SELECT x1."primary_type" as "primary_type",x1."total" as "total",_y."year" as "year",_y."avg_temperature" as "avg_temperature" FROM (SELECT "primary_type" as "primary_type","total" as "total" FROM (SELECT "primary_type" as "primary_type",(count(*)::numeric) as "total" FROM (SELECT t1.case_number as "case_number",t1.primary_type as "primary_type" FROM t1) AS x1 GROUP BY "primary_type") AS x1) AS x1 JOIN (SELECT x1."year" as "year",x1."avg_temperature" as "avg_temperature" FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1 WHERE (x1."avg_temperature" > ?)) as _y ON ((_y."year" = x1."total") and (x1."total" = ?))) AS x1""")
    val paramsChainSelectMax = setParams.map { (setParam) => setParam(None, 0).get }
    paramsChainSelectMax should be (params)
  }

  test("parameters should align with joins") {
    val v1soql = "SELECT year, avg_temperature FROM @year |> SELECT coalesce(max(avg_temperature), 1), year WHERE avg_temperature < 2 GROUP BY year"
    val v2soql = "SELECT year, avg_temperature FROM @year |> SELECT coalesce(sum(avg_temperature), 3), year WHERE avg_temperature < 4 GROUP BY year"

    val soql =
      s"""
         | SELECT case_number
         | JOIN ($v1soql) AS j1 ON year = @j1.year
         | JOIN ($v2soql) AS j2 ON year = @j2.year
         """.stripMargin
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)

    val expectedSql = """SELECT t1.case_number FROM t1
      |     JOIN
      |  (SELECT (coalesce((max("avg_temperature")),?)) as "coalesce_max_avg_temperature_1","year" as "year"
      |     FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1
      |    WHERE ("avg_temperature" < ?) GROUP BY "year") as _j1 ON (t1.year = _j1."year")
      |     JOIN
      |  (SELECT (coalesce((sum("avg_temperature")),?)) as "coalesce_sum_avg_temperature_3","year" as "year"
      |     FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1
      |    WHERE ("avg_temperature" < ?) GROUP BY "year") as _j2 ON (t1.year = _j2."year")""".stripMargin

    collapseSpace(sql) should be (collapseSpace(expectedSql))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, 2, 3, 4).map(BigDecimal(_)))
  }

  test("parameters should align with joins and outer chain") {
    val v1soql = "SELECT year, avg_temperature FROM @year |> SELECT coalesce(max(avg_temperature), 1), year WHERE avg_temperature < 2 GROUP BY year"
    val v2soql = "SELECT year, avg_temperature FROM @year |> SELECT coalesce(sum(avg_temperature), 3), year WHERE avg_temperature < 4 GROUP BY year"

    val soql =
      s"""SELECT case_number
            JOIN ($v1soql) AS j1 ON year = @j1.year
            JOIN ($v2soql) AS j2 ON year = @j2.year
          |> SELECT case_number, 11 as c WHERE 12 = 13
         """
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)

    val expectedSql = """SELECT "case_number",? FROM (SELECT t1.case_number as "case_number" FROM t1
                                  JOIN
                                       (SELECT (coalesce((max("avg_temperature")),?)) as "coalesce_max_avg_temperature_1","year" as "year"
                                          FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1
                                         WHERE ("avg_temperature" < ?) GROUP BY "year") as _j1 ON (t1.year = _j1."year")
                                  JOIN
                                       (SELECT (coalesce((sum("avg_temperature")),?)) as "coalesce_sum_avg_temperature_3","year" as "year"
                                          FROM (SELECT t3.year as "year",t3.avg_temperature as "avg_temperature" FROM t3) AS x1
                                         WHERE ("avg_temperature" < ?) GROUP BY "year") as _j2 ON (t1.year = _j2."year")) AS x1 WHERE (? = ?)"""

    collapseSpace(sql) should be (collapseSpace(expectedSql))

    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(11, 1, 2, 3, 4, 12, 13).map(BigDecimal(_)))
  }

  test("this alias") {
    val soql = "SELECT @t.id FROM @this as t"
    val expected = "SELECT _t.id FROM t1 as _t"
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("this alias in chained soql") {
    val soql = "select id |> select @t.id from @this as t"
    val expected = """SELECT _t."id" FROM (SELECT t1.id as "id" FROM t1) as _t"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("join chained soql and multiple this's") {
    val soql = "select @t.id from @this as t join (select name from @cat |> select @tc.name from @this as tc ) as c on true"
    val expected = """SELECT _t.id FROM t1 as _t JOIN (SELECT _tc."name" as "name" FROM (SELECT "name_45" as "name" FROM t11) as _tc) as _c ON ?"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("lateral join and this") {
    val soql = "select @t.id from @this as t join lateral (select name, @t.id from @cat) as c on true"
    val expected = """SELECT _t.id FROM t1 as _t JOIN LATERAL (SELECT "name_45" as "name",_t.id as "id" FROM t11) as _c ON ?"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("lateral join chained soql and this") {
    val soql = "select @t.id from @this as t join @dog as d on true join lateral (select name from @cat |> select name, @d.dog) as c on true"
    val expected = """SELECT _t.id FROM t1 as _t JOIN t12 as _d ON ?  JOIN LATERAL (SELECT "name" as "name",_d."dog_58" as "dog" FROM (SELECT "name_45" as "name" FROM t11) AS x1) as _c ON ?"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("multiple unions and this's") {
    val soql =
      """SELECT @t.primary_type FROM @this as t UNION
         (SELECT breed, cat FROM @cat |> SELECT @c.cat FROM @this as c) UNION
         (SELECT breed, dog FROM @dog |> SELECT @d.dog FROM @this as d)"""
    val expected = """SELECT _t.primary_type FROM t1 as _t UNION (SELECT _c."cat" FROM (SELECT "breed_46" as "breed","cat_48" as "cat" FROM t11) as _c) UNION (SELECT _d."dog" FROM (SELECT "breed_56" as "breed","dog_58" as "dog" FROM t12) as _d)"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("all query ops") {
    val soql =
      """SELECT @t.primary_type FROM @this as t MiNuS
         SELECT name FROM @cat intERSECT
         SELECT name FROM @dog UNioN aLL
         SELECT name FROM @bird UnioN
         SELECT name FROM @fish"""
    val expected = """SELECT _t.primary_type FROM t1 as _t EXCEPT SELECT "name_45" FROM t11 INTERSECT SELECT "name_55" FROM t12 UNION ALL SELECT "name_65" FROM t13 UNION SELECT "name_65" FROM t14"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive)
    sql should be (expected)
  }

  test("generate ':id' in system columns in the right side of a chained soql with this alias and join") {
    val soql = "SELECT @t.:id FROM @this as t |> SELECT @t2.:id, @d.:id as id2 FROM @this as t2 JOIN @dog as d ON true"
    val expected = """SELECT _t2.":id",_d.":id_51" FROM (SELECT _t.":id_1" as ":id" FROM t1 as _t) as _t2 JOIN t12 as _d ON ?"""
    val ParametricSql(Seq(sql), _) = sqlize(soql, CaseSensitive, useRepsWithId = true)
    println(sql)
    sql should be (expected)
  }

  private def collapseSpace(s: String): String = {
    s.replaceAll("\\s+", " ")
  }
}
