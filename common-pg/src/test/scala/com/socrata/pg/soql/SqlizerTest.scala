package com.socrata.pg.soql

import org.scalatest.{Matchers, FunSuite}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.pg.store.index.SoQLIndexableRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

// scalastyle:off null
class SqlizerTest extends FunSuite with Matchers {
  import SqlizerTest._

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
    sql should be ("SELECT case_number FROM t1 WHERE (case_number in(?,?,?)) ORDER BY case_number nulls last LIMIT 2 OFFSET 1")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("ha001", "ha002", "ha003"))
  }

  test("field in (x, y...) ci") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT case_number FROM t1 WHERE (upper(case_number) in(?,?,?)) ORDER BY upper(case_number) nulls last LIMIT 2 OFFSET 1")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("HA001", "HA002", "HA003"))
  }

  test("point/line/polygon") {
    val soql = "select case_number, point, line, polygon"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT case_number,ST_AsBinary(point),ST_AsBinary(line),ST_AsBinary(polygon) FROM t1")
    setParams.length should be (0)
  }

  test("extent") {
    val soql = "select extent(point), extent(multiline), extent(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Multi(ST_Extent(point)))),ST_AsBinary((ST_Multi(ST_Extent(multiline)))),ST_AsBinary((ST_Multi(ST_Extent(multipolygon)))) FROM t1")
    setParams.length should be (0)
  }

  test("concave hull") {
    val soql = "select concave_hull(point, 0.99), concave_hull(multiline, 0.89), concave_hull(multipolygon, 0.79)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union(point), ?))))," +
                          "ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union(multiline), ?))))," +
                          "ST_AsBinary((ST_Multi(ST_ConcaveHull(ST_Union(multipolygon), ?)))) FROM t1")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(0.99, 0.89, 0.79).map(BigDecimal(_)))
  }

  test("convex hull") {
    val soql = "select convex_hull(point), convex_hull(multiline), convex_hull(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union(point)))))," +
                          "ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union(multiline)))))," +
                          "ST_AsBinary((ST_Multi(ST_ConvexHull(ST_Union(multipolygon))))) FROM t1")
    setParams.length should be (0)
  }

  test("intersects") {
    val soql = "select intersects(point, 'MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_Intersects(point, (ST_GeomFromText(?, 4326)))) FROM t1")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)))"))
  }

  test("distance in meters") {
    val soql = "select distance_in_meters(point, 'POINT(0 0)')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_Distance(point::geography, (ST_GeomFromText(?, 4326))::geography)) FROM t1")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("POINT(0 0)"))
  }

  test("is empty") {
    val soql = "select is_empty(multipolygon)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected = "SELECT (ST_IsEmpty(multipolygon) or multipolygon is null) FROM t1"
    sql.replaceAll("\\s+", " ") should be (expected)
    setParams.length should be (0)
  }

  test("visible at") {
    val soql = "select visible_at(multipolygon, 0.03)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    val expected =
      """SELECT ((NOT ST_IsEmpty(multipolygon))
        |     AND (ST_GeometryType(multipolygon) = 'ST_Point'
        |     OR ST_GeometryType(multipolygon) = 'ST_MultiPoint'
        |     OR (ST_XMax(multipolygon) - ST_XMin(multipolygon)) >= ?
        |     OR (ST_YMax(multipolygon) - ST_YMin(multipolygon)) >= ?) )
        | FROM t1""".stripMargin.replaceAll("\\s+", " ")
    sql.replaceAll("\\s+", " ") should be (expected)
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(0.03, 0.03).map(BigDecimal(_)))
  }

  test("expr and expr") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE ((id = ?) and (case_number = ?))")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, "cn001"))
  }

  test("expr and expr ci") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT id FROM t1 WHERE ((id = ?) and (upper(case_number) = ?))")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, "CN001"))
  }

  test("starts_with has automatic suffix %") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE (case_number like (? || ?))")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("cn", "%"))
  }

  test("starts_with has automatic suffix % ci") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseInsensitive)
    sql should be ("SELECT id FROM t1 WHERE (upper(case_number) like (? || ?))")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("CN", "%"))
  }

  test("between") {
    val soql = "select id where id between 1 and 9"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE (id between ? and ?)")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, 9))
  }

  test("select count(*)") {
    val soql = "select count(*)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(*)) FROM t1")
    setParams.length should be (0)
  }

  test("select aggregate functions") {
    val soql = "select count(id), avg(id), min(id), max(id), sum(id)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (count(id)),(avg(id)),(min(id)),(max(id)),(sum(id)) FROM t1")
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

  test("search") {
    val soql = "select id search 'oNe Two'"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT id FROM t1 WHERE to_tsvector('english', coalesce(array_12,'') || ' ' || coalesce(case_number_6,'') || ' ' || coalesce(object_11,'') || ' ' || coalesce(primary_type_7,'')) @@ plainto_tsquery('english', ?)")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("oNe Two"))
  }

  test("signed magnitude 10") {
    val soql = "select signed_magnitude_10(year)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (sign(year) * length(floor(abs(year))::text)) FROM t1")
    setParams.length should be (0)
  }

  test("signed magnitude linear") {
    val soql = "select signed_magnitude_linear(year, 42)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be("SELECT (case when ? = 1 then floor(year) else sign(year) * floor(abs(year)/? + 1) end) FROM t1")
    setParams.length should be(2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(42,42))
  }

  test("case fn") {
    val soql = "select case(primary_type = 'A', 'X', primary_type = 'B', 'Y')"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (case WHEN (primary_type = ?) THEN ? WHEN (primary_type = ?) THEN ? end) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("A", "X", "B", "Y"))
  }

  test("simplify multigeometry") {
    val soql = "select simplify(multipolygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Multi(ST_Simplify(multipolygon, ?)))) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify geometry") {
    val soql = "select simplify(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Simplify(polygon, ?))) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify multigeometry preserving topology") {
    val soql = "select simplify_preserve_topology(multipolygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_Multi(ST_SimplifyPreserveTopology(multipolygon, ?)))) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("simplify geometry preserving topology") {
    val soql = "select simplify_preserve_topology(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_SimplifyPreserveTopology(polygon, ?))) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("geometry snap to grid") {
    val soql = "select snap_to_grid(polygon, 0.5)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((ST_SnapToGrid(polygon, ?))) FROM t1")
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be(Seq(0.5))
  }

  test("curated region test") {
    val soql = "select curated_region_test(multipolygon, 20)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql.replaceAll("""\s+""", " ") should be ("""SELECT
                  (case when st_npoints(multipolygon) > ? then 'too complex'
                        when st_xmin(multipolygon) < -180 or st_xmax(multipolygon) > 180 or st_ymin(multipolygon) < -90 or st_ymax(multipolygon) > 90 then 'out of bounds'
                        when not st_isvalid(multipolygon) then st_isvalidreason(multipolygon)::text
                        when (multipolygon) is null then 'empty'
                   end ) FROM t1""".replaceAll("""\s+""", " "))
    setParams.length should be (1)
  }

  test("location latitude") {
    val soql = "select location_latitude(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_Y(location_geom)::numeric) FROM t1")
    setParams.length should be (0)
  }

  test("location longitude") {
    val soql = "select location_longitude(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (ST_X(location_geom)::numeric) FROM t1")
    setParams.length should be (0)
  }

  test("location address") {
    val soql = "select location_address(location)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT (location_address) FROM t1")
    setParams.length should be (0)
  }

  test("location point") {
    val soql = "select location::point"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be ("SELECT ST_AsBinary((location_geom)) FROM t1")
    setParams.length should be (0)
  }

  test("location within_circle") {
    val soql = "select case_number where within_circle(location, 1.0, 2.0, 30)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be (
      """SELECT case_number FROM t1
        | WHERE ((ST_within((location_geom), ST_Buffer(ST_MakePoint(?, ?)::geography, ?)::geometry)))"""
        .stripMargin.replaceAll("\n", ""))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(2.0, 1.0, 30d).map(BigDecimal.valueOf(_)))
  }

  test("location within_box") {
    val soql = "select case_number where within_box(location, 1.0, 2.0, 3.0, 4.0)"
    val ParametricSql(Seq(sql), setParams) = sqlize(soql, CaseSensitive)
    sql should be (
      """SELECT case_number FROM t1
        | WHERE ((ST_MakeEnvelope(?, ?, ?, ?, 4326) ~ (location_geom)))"""
        .stripMargin.replaceAll("\n", ""))
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(2.0, 3.0, 4.0, 1.0).map(BigDecimal.valueOf(_)))
  }
}

object SqlizerTest {
  private val cryptProvider = new CryptProvider(CryptProvider.generateKey())
  val sqlCtx = Map[SqlizerContext, Any](
    SqlizerContext.IdRep -> new SoQLID.StringRep(cryptProvider),
    SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider)
  )

  private def sqlize(soql: String, caseSensitivity: CaseSensitivity): ParametricSql = {
    val allColumnReps = columnInfos.map(PostgresUniverseCommon.repForIndex(_))
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    SoQLAnalysisSqlizer.sql((analysis, "t1", allColumnReps))(
      columnReps,
      Seq.empty,
      sqlCtx + (SqlizerContext.CaseSensitivity -> caseSensitivity),
      passThrough)
  }

  private val idMap =  (cn: ColumnName) => new UserColumnId(cn.caseFolded)

  private val passThrough = (s: String) => s

  private val columnMap = Map(
    ColumnName(":id") -> ((1, SoQLID)),
    ColumnName(":version") -> ((2, SoQLVersion)),
    ColumnName(":created_at") -> ((3, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((4, SoQLFixedTimestamp)),
    ColumnName("id") -> ((5, SoQLNumber)),
    ColumnName("case_number") -> ((6, SoQLText)),
    ColumnName("primary_type") -> ((7, SoQLText)),
    ColumnName("year") -> ((8, SoQLNumber)),
    ColumnName("arrest") -> ((9, SoQLBoolean)),
    ColumnName("updated_on") -> ((10, SoQLFloatingTimestamp)),
    ColumnName("object") -> ((11, SoQLObject)),
    ColumnName("array") -> ((12, SoQLArray)),
    ColumnName("point") -> ((13, SoQLPoint)),
    ColumnName("multiline") -> ((14, SoQLMultiLine)),
    ColumnName("multipolygon") -> ((15, SoQLMultiPolygon)),
    ColumnName("polygon") -> ((16, SoQLPolygon)),
    ColumnName("line") -> ((17, SoQLLine)),
    ColumnName("multipoint") -> ((18, SoQLMultiPoint)),
    ColumnName("location") -> ((19, SoQLLocation))
  )

  private val columnInfos = columnMap.foldLeft(Seq.empty[ColumnInfo[SoQLType]]) { (acc, colNameAndType) => colNameAndType match {
    case (columnName: ColumnName, (id, typ)) =>
      val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType](
        null,
        new ColumnId(id),
        new UserColumnId(columnName.caseFolded),
        typ,
        columnName.caseFolded,
        typ == SoQLID,
        false, // isUserKey
        typ == SoQLVersion
      )(SoQLTypeContext.typeNamespace, null)
      acc :+ cinfo
  }}

  private val datasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](columnMap,  columnMap.keys.toVector)
  }

  private val columnReps = {
    columnMap.map { case (colName, (_, typ)) =>
      idMap(colName) -> SoQLIndexableRep.sqlRep(typ, colName.name)
    }
  }
}
