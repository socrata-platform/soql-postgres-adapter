package com.socrata.pg.soql

import SqlizerTest._
import com.socrata.soql.exceptions.TypecheckException
import com.socrata.soql.types.SoQLID

// scalastyle:off null
class SqlizerBasicTest extends SqlizerTest {
  test("string literal with quotes") {
    val soql = "select 'there is a '' quote'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ? FROM t1")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("there is a ' quote" ))
  }

  test("field in (x, y...)") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".case_number FROM t1 WHERE ("t1".case_number in(?,?,?)) ORDER BY "t1".case_number nulls last LIMIT 2 OFFSET 1""")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("ha001", "ha002", "ha003"))
  }

  test("field in (x, y...) ci") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("""SELECT "t1".case_number FROM t1 WHERE (upper("t1".case_number) in(?,?,?)) ORDER BY upper("t1".case_number) nulls last LIMIT 2 OFFSET 1""")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("HA001", "HA002", "HA003"))
  }

  test("point/line/polygon") {
    val soql = "select case_number, point, line, polygon"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".case_number,ST_AsBinary("t1".point),ST_AsBinary("t1".line),ST_AsBinary("t1".polygon) FROM t1""")
    setParams.length should be (0)
  }

  test("extent") {
    val soql = "select extent(point), extent(multiline), extent(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Multi(ST_Extent("t1".point)))),ST_AsBinary((ST_Multi(ST_Extent("t1".multiline)))),ST_AsBinary((ST_Multi(ST_Extent("t1".multipolygon)))) FROM t1""")
    setParams.length should be (0)
  }

  test("concave hull") {
    val soql = "select concave_hull(point, 0.99), concave_hull(multiline, 0.89), concave_hull(multipolygon, 0.79)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union("t1".point), ?)))),""" +
      """ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union("t1".multiline), ?)))),""" +
      """ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union("t1".multipolygon), ?)))) FROM t1""")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(0.99, 0.89, 0.79).map(BigDecimal(_)))
  }

  test("convex hull") {
    val soql = "select convex_hull(point), convex_hull(multiline), convex_hull(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union("t1".point))))),""" +
      """ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union("t1".multiline))))),""" +
      """ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union("t1".multipolygon))))) FROM t1""")
    setParams.length should be (0)
  }

  test("intersects") {
    val soql = "select intersects(point, 'MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (ST_Intersects("t1".point, (ST_GeomFromText(?, 4326)))) FROM t1""")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))"))
  }

  test("distance in meters") {
    val soql = "select distance_in_meters(point, 'POINT(0 0)')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (ST_Distance("t1".point::geography, (ST_GeomFromText(?, 4326))::geography)::numeric) FROM t1""")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("POINT(0 0)"))
  }

  test("number of points") {
    val soql = "select num_points(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (ST_NPoints("t1".multipolygon)) FROM t1""")
    setParams.length should be (0)
  }

  test("is empty") {
    val soql = "select is_empty(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = """SELECT (ST_IsEmpty("t1".multipolygon) or "t1".multipolygon is null) FROM t1"""
    sql.replaceAll("\\s+", " ") should be (expected)
    setParams.length should be (0)
  }

  test("visible at") {
    val soql = "select visible_at(multipolygon, 0.03)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected =
      """SELECT ((NOT ST_IsEmpty("t1".multipolygon))
        |     AND (ST_GeometryType("t1".multipolygon) = 'ST_Point'
        |     OR ST_GeometryType("t1".multipolygon) = 'ST_MultiPoint'
        |     OR (ST_XMax("t1".multipolygon) - ST_XMin("t1".multipolygon)) >= ?
        |     OR (ST_YMax("t1".multipolygon) - ST_YMin("t1".multipolygon)) >= ?) )
        | FROM t1""".stripMargin.replaceAll("\\s+", " ")
    sql.replaceAll("\\s+", " ") should be (expected)
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(0.03, 0.03).map(BigDecimal(_)))
  }

  test("expr and expr") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE (("t1".id = ?) and ("t1".case_number = ?))""")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, "cn001"))
  }

  test("expr and expr ci") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE (("t1".id = ?) and (upper("t1".case_number) = ?))""")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, "CN001"))
  }

  test("starts_with has automatic suffix %") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE ("t1".case_number like (? || ?))""")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("cn", "%"))
  }

  test("starts_with has automatic suffix % ci") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE (upper("t1".case_number) like (? || ?))""")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("CN", "%"))
  }

  test("between") {
    val soql = "select id where id between 1 and 9"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE ("t1".id between ? and ?)""")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, 9))
  }

  test("select count(*)") {
    val soql = "select count(*)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(*)::numeric) FROM t1")
    setParams.length should be (0)
  }

  test("select aggregate functions") {
    val soql = "select count(id), avg(id), min(id), max(id), sum(id), median(id), median(case_number)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ((count("t1".id))::numeric),(avg("t1".id)),(min("t1".id)),(max("t1".id)),(sum("t1".id)),(percentile_cont(.50) within group (order by "t1".id)),(percentile_disc(.50) within group (order by "t1".case_number)) FROM t1""")
    setParams.length should be (0)
  }

  test("select median inside window function") {
    val soql = "select median(id) over (partition by primary_type, year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (median_ulib_agg("t1".id) over( partition by "t1".primary_type,"t1".year)) FROM t1""")
    setParams.length should be (0)
  }

  test("select text and number conversions") {
    val soql = "select 123::text, '123'::number"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (?::varchar),(?::numeric) FROM t1")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(123, "123"))
  }

  test("substring start parameter only") {
    val soql = "select substring(case_number, 1)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (substring("t1".case_number, ?::int)) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1))
  }

  test("substring two parameters") {
    val soql = "select substring(case_number, 1, 2)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (substring("t1".case_number, ?::int, ?::int)) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, 2))
  }

  test("order by literal is skipped") {
    val soql = "SELECT 'aa' || 'bb' as aabb, case_number ORDER BY aabb, case_number"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (? || ?),"t1".case_number FROM t1 ORDER BY "t1".case_number nulls last""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("aa", "bb"))
  }

  test("order by literal is skipped and no incomplete order by") {
    val soql = "SELECT 'aa' || 'bb' as aabb, case_number ORDER BY aabb"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (? || ?),"t1".case_number FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("aa", "bb"))
  }

  test("count(*) is non-literal and kept in order by") {
    val soqls = Seq("SELECT case_number, count(*) GROUP BY case_number ORDER BY count(*)",
                    "SELECT case_number, count(*) as ct GROUP BY case_number ORDER BY ct")
    soqls.foreach { soql =>
      val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
      sql should be("""SELECT "t1".case_number,(count(*)::numeric) FROM t1 GROUP BY "t1".case_number ORDER BY (count(*)::numeric) nulls last""")
      setParams.length should be(0)
    }
  }

  test("search") {
    val soql = "select * search 'oNe Two'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive, useRepsWithId = true)
    sql should be ("""SELECT "t1".array_12,"t1".object_11,ST_AsBinary("t1".multipoint_18),"t1".phone_20_number,"t1".phone_20_type,"t1".year_8,ST_AsBinary("t1".multiline_14),"t1".json_23,"t1".arrest_9,ST_AsBinary("t1".multipolygon_15),ST_AsBinary("t1".polygon_16),"t1".case_number_6,"t1".updated_on_10,ST_AsBinary("t1".point_13),"t1".id_5,"t1".document_22,ST_AsBinary("t1".location_19_geom),"t1".location_19_address,"t1".primary_type_7,"t1".url_21_url,"t1".url_21_description,ST_AsBinary("t1".line_17) FROM t1 WHERE (to_tsvector('english', coalesce("t1".array_12,'') || ' ' || coalesce("t1".case_number_6,'') || ' ' || coalesce("t1".object_11,'') || ' ' || coalesce("t1".primary_type_7,'') || ' ' || coalesce("t1".url_21_description,'') || ' ' || coalesce("t1".url_21_url,'')) @@ plainto_tsquery('english', ?))""")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("oNe Two"))
  }

  test("signed magnitude 10") {
    val soql = "select signed_magnitude_10(year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (sign("t1".year) * length(floor(abs("t1".year))::text)) FROM t1""")
    setParams.length should be (0)
  }

  test("signed magnitude linear") {
    val soql = "select signed_magnitude_linear(year, 42)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be("""SELECT (case when ? = 1 then floor("t1".year) else sign("t1".year) * floor(abs("t1".year)/? + 1) end) FROM t1""")
    setParams.length should be(2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(42,42))
  }

  test("date_extract_hh") {
    val soql = "select date_extract_hh(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(hour from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_dow - day of week") {
    val soql = "select date_extract_dow(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(dow from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_woy - week of year") {
    val soql = "select date_extract_woy(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(week from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_iso_y - iso year") {
    val soql = "select date_extract_iso_y(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(isoyear from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_y") {
    val soql = "select date_extract_y(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(year from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_m") {
    val soql = "select date_extract_m(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(month from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_extract_d") {
    val soql = "select date_extract_d(updated_on)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (extract(day from "t1".updated_on)::numeric) FROM t1""")
    setParams.length should be (0)
  }

  test("date_add") {
    val soql = "select date_add(updated_on, 'P1DT1H') as d1, date_add(updated_on, 'PT86401S') as d2, updated_on + 'P1D' as d3"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ("t1".updated_on + (?::interval)),("t1".updated_on + (?::interval)),("t1".updated_on + (?::interval)) FROM t1""")
    val params = setParams.map { setParam => setParam(None, 0).get }
    params should be(Seq("1 day 1 hour", "86401.000 seconds", "1 day"))
  }

  test("to_floating_timestamp") {
    val soql = "select date_trunc_ymd(to_floating_timestamp(:created_at, 'PST'))"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (date_trunc('day', ("t1".:created_at at time zone ?))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("PST"))
  }

  test("case fn") {
    val soql = "select case(primary_type = 'A', 'X', primary_type = 'B', 'Y')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (case WHEN ("t1".primary_type = ?) THEN ? WHEN ("t1".primary_type = ?) THEN ? end) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("A", "X", "B", "Y"))
  }

  test("coalesce") {
    val soql = "select coalesce(case_number, primary_type, 'default')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (coalesce("t1".case_number,"t1".primary_type,?)) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("default"))
  }

  test("nullif") {
    val soql = "select nullif(case_number, primary_type)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT (nullif("t1".case_number,"t1".primary_type)) FROM t1""")
    setParams.size should be (0)
  }

  test("coalesce parameters must have the same type") {
    val soql = "select coalesce(primary_type, 123)"
    val ex = intercept[TypecheckException] {
      sqlize(soql, CaseSensitive)
    }
    ex.getMessage should be("Cannot pass a value of type `number' to function `coalesce':\nselect coalesce(primary_type, 123)\n                              ^")
  }

  test("simplify multigeometry") {
    val soql = "select simplify(multipolygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Multi(ST_Simplify("t1".multipolygon, ?)))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify geometry") {
    val soql = "select simplify(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Simplify("t1".polygon, ?))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify multigeometry preserving topology") {
    val soql = "select simplify_preserve_topology(multipolygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_Multi(ST_SimplifyPreserveTopology("t1".multipolygon, ?)))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify geometry preserving topology") {
    val soql = "select simplify_preserve_topology(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_SimplifyPreserveTopology("t1".polygon, ?))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("geometry snap to grid") {
    val soql = "select snap_to_grid(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT ST_AsBinary((ST_SnapToGrid("t1".polygon, ?))) FROM t1""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("curated region test") {
    val soql = "select curated_region_test(multipolygon, 20)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql.replaceAll("""\s+""", " ") should be ("""SELECT
                  (case when st_npoints("t1".multipolygon) > ? then 'too complex'
                        when st_xmin("t1".multipolygon) < -180 or st_xmax("t1".multipolygon) > 180 or st_ymin("t1".multipolygon) < -90 or st_ymax("t1".multipolygon) > 90 then 'out of bounds'
                        when not st_isvalid("t1".multipolygon) then st_isvalidreason("t1".multipolygon)::text
                        when ("t1".multipolygon) is null then 'empty'
                   end ) FROM t1""".replaceAll("""\s+""", " "))
    setParams.length should be (1)
  }

  test("group by literals with constants removed") {
    val soql = "select id, 'stRing' as a, 5 as b, 2*3 as c group by id, a, b, c"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".id,e'stRing',5,(2 * 3) FROM t1 GROUP BY "t1".id""")
    setParams.length should be (0)
  }

  /**
    * This is an edge case of a pretty useless query.
    */
  test("group by all literals and constants stay when everything is a constant") {
    val soql = "select 'stRing' as a, 5 as b, 2*3 as c group by a, b, c"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT e'stRing',5,(2 * 3) FROM t1 GROUP BY 1,2,3")
    setParams.length should be (0)
  }

  test("distinct") {
    val soql = "select distinct case_number, primary_type"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT DISTINCT "t1".case_number,"t1".primary_type FROM t1""")
    setParams.length should be (0)
  }

  test("row id") {
    val rep = sqlCtx(SqlizerContext.IdRep).asInstanceOf[SoQLID.StringRep]
    val rowId = 2
    val encodedRowId = rep.apply(SoQLID(rowId))
    val soql = s"select id where :id = '$encodedRowId'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("""SELECT "t1".id FROM t1 WHERE (":id_1" = (?))""")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(rowId))
  }

  test("window function") {
    val soql = "select avg(year) over(partition by primary_type, year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (avg("t1".year) over( partition by "t1".primary_type,"t1".year)) FROM t1""")
    setParams.length should be (0)
  }

  test("window function empty over") {
    val soql = "select avg(year) over()"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (avg("t1".year) over()) FROM t1""")
    setParams.length should be (0)
  }

  test("window function rank over") {
    val soql = "select rank() over(order by year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (rank() over( order by "t1".year nulls last)) FROM t1""")
    setParams.length should be (0)
  }

  test("window function with frame clause") {
    val soql = "select avg(year) over(partition by primary_type, year ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (avg("t1".year) over( partition by "t1".primary_type,"t1".year ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) FROM t1""")
    setParams.length should be (0)
  }

  test("count(column) filter has proper numeric cast") {
    val soql = "select count(year) filter (where year = year and true)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (count("t1".year) filter(where (("t1".year = "t1".year) and ?) )::numeric) FROM t1""")
    params should be (Seq(true))
  }

  test("count(*) filter + window function has proper numeric cast") {
    val soql = "select count(*) filter (where year = year and true) over(partition by primary_type, year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (count(*) filter(where (("t1".year = "t1".year) and ?) ) over( partition by "t1".primary_type,"t1".year)::numeric) FROM t1""")
    params should be (Seq(true))
  }

  test("filter + window function") {
    val soql = "select avg(year) filter (where year = year and true) over(partition by primary_type, year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("""SELECT (avg("t1".year) filter(where (("t1".year = "t1".year) and ?) ) over( partition by "t1".primary_type,"t1".year)) FROM t1""")
    params should be (Seq(true))
  }

  test("json subscript") {
    val soql = "select json.foo"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    sql should be ("SELECT (\"t1\".json -> ?) FROM t1")
    params should be (Seq("foo"))

  }

  test("simple single row does not generate a from clause") {
    val soql = "select 'bleh' from @single_row"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ?")
  }
}
