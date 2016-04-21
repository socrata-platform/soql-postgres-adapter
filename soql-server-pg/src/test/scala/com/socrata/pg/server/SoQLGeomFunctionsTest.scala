package com.socrata.pg.server

class SoQLGeomFunctionsTest extends SoQLTest {

  test("within_circle(point, lat, lon, m)") {
    compareSoqlResult("select make, name, point where within_circle(point, 32.50578, 34.8926, 10000) order by name", "where-within-circle.json")
  }

  test("within_circle lat lon reversed") {
    compareSoqlResult("select make, name, point where within_circle(point, 34.8926, 32.50578, 10000) order by name", "empty.json")
  }

  test("within_polygon matching apco") {
    compareSoqlResult("select make, name, point where within_polygon(point, 'POLYGON ((34.8 32.4, 35.0 32.4, 35.0 32.6, 34.8 32.6, 34.8 32.4))') order by name", "where-within-polygon.json")
  }

  test("within_polygon matching apco (multipolygon)") {
    compareSoqlResult("select make, name, point where within_polygon(point, 'MULTIPOLYGON(((34.8 32.4, 35.0 32.4, 35.0 32.6, 34.8 32.6, 34.8 32.4)))') order by name", "where-within-polygon.json")
  }

  test("within_polygon no matches") {
    compareSoqlResult("select make, name, point where within_polygon(point, 'MULTIPOLYGON(((1 1, 2 1, 2 2, 1 2, 1 1)))') order by name", "empty.json")
  }

  test("within_box(point, lat_tl, lon_tl, lat_br, lon_br)") {
    // sharing the same result file as within-circle
    compareSoqlResult("select make, name, point where within_box(point, 33, 34, 32, 35) order by name", "where-within-circle.json")
  }

  test("within_box lat lon reversed") {
    compareSoqlResult("select make, name, point where within_box(point, 34, 33, 35, 32)", "empty.json")
  }

  test("point conversion") {
    compareSoqlResult("select name, 'point (1.1 2.2)'::point as northeast, 'pOInt(-1.1 -2.2)'::point as southwest where name = 'Chili'", "select-point-lit.json")
  }

  test("multipolygon conversion") {
    compareSoqlResult("select name, 'multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon as multipolygon where name = 'Chili'", "select-mpolygon-lit.json")
  }

  test("multiline conversion") {
    compareSoqlResult("select name, 'multilinestring((10.123456 -20.123456, -30.654321 40.654321))'::multiline as multiline where name = 'Chili'", "select-mline-lit.json")
  }

  test("line conversion") {
    compareSoqlResult("select name, 'linestring(100.123456 0.123456, 100.123456 100.123456)'::line as line where name = 'Chili'", "select-line-lit.json")
  }

  test("multipoint conversion") {
    compareSoqlResult("select name, 'multipoint((10.123456 -20.123456), (-30.654321 40.654321))'::multipoint as multipoint where name = 'Chili'", "select-multipoint-lit.json")
  }

  test("polygon conversion") {
    compareSoqlResult("select name, 'polygon ((30.123456 10.123456, 40.123456 40.123456, 20.123456 40.123456, 10.123456 20.123456, 30.123456 10.123456))'::polygon as polygon where name = 'Chili'", "select-polygon-lit.json")
  }

  test("extent") {
    compareSoqlResult("select extent(point) as extent where make = 'APCO' or make = 'Skywalk' ", "select-extent.json")
  }

  test("extent groupby") {
    compareSoqlResult("select size, extent(point) as extent where size='Small' group by size", "select-extent-groupby.json")
  }

  test("concave hull") {
    compareSoqlResult("select concave_hull(multipolygon, 0.99) as concave_hull where country = 'Germany'", "select-concave-hull.json")
  }

  test("convex hull") {
    compareSoqlResult("select convex_hull(multipolygon) as convex_hull where country = 'Germany'", "select-convex-hull.json")
  }

  test("intersects") {
    // The multipolygon below covers the following cases:
    // - Shapes fully contained within the multipolygon
    // - Shapes overlapping but not fully contained within the multipolygon
    // - Shapes on the boundary of the multipolygon
    compareSoqlResult("select make, name, multipolygon where intersects(multipolygon, 'MULTIPOLYGON(((0 0, 1.5 0, 1.5 2, 0 2, 0 0)))')", "where-intersects.json")
  }

  test("intersects no matches") {
    compareSoqlResult("select make, name, multipolygon where intersects(multipolygon, 'MULTIPOLYGON(((2 3,3 3,3 4,2 4,2 3)))')", "empty.json")
  }

  test("distance with literals") {
    // Distance between London and New York
    compareSoqlResult("select distance_in_meters('POINT(-0.127691 51.517320)', 'POINT(-73.976248 40.767049)') " +
                        "as distance_in_meters limit 1", "select-distance-literal.json")
  }

  test("distance with columns") {
    compareSoqlResult("select distance_in_meters(point, multipolygon) as distance_in_meters where code = 'SPRINT-EVO'", "select-distance-columns.json")
  }

  test("numpoints with columns") {
    compareSoqlResult("select num_points(multipolygon) as num_points where code = 'SPRINT-EVO'", "select-num-points.json")
  }

  test("numpoints with literals") {
    compareSoqlResult(
      "select num_points('MULTIPOLYGON(((11.578791 48.144026,11.582921 48.143088,11.582181 48.141649,11.578136 48.142515,11.578791 48.144026)))') " +
        "as num_points " +
        "where code = 'SPRINT-EVO'",
      "select-num-points.json")
  }

  test("simplify geometry") {
    compareSoqlResult(
      """select name,
                simplify('polygon((1 1, 1.5 1, 2 1, 2 1.5,  2 2, 1.5 2,  1 2, 1 1.5, 1 1))'::polygon, 0.5) as simplified,
                'polygon((1 1, 2 1, 2 2, 1 2, 1 1))'::polygon as original
          where name = 'Chili'""",
      "select-simplify-geometry.json")
  }

  test("simplify geometry and preserve multipolygon") {
    compareSoqlResult(
      """select name,
                simplify('multipolygon(((1 1, 1.5 1, 2 1, 2 1.5,  2 2, 1.5 2,  1 2, 1 1.5, 1 1)))'::multipolygon, 0.5) as simplified,
                'multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon as original
          where name = 'Chili'""",
      "select-simplify-multigeometry.json")
  }

  test("simplify geometry preserving topology") {
    compareSoqlResult(
      """select name,
                simplify_preserve_topology('polygon((1 1, 1.5 1, 2 1, 2 1.5,  2 2, 1.5 2,  1 2, 1 1.5, 1 1))'::polygon, 0.5) as simplified,
                'polygon((1 1, 2 1, 2 2, 1 2, 1 1))'::polygon as original
          where name = 'Chili'""",
      "select-simplify-geometry.json")
  }

  test("simplify (preserving topology) geometry and preserve multipolygon") {
    compareSoqlResult(
      """select name,
                simplify_preserve_topology('multipolygon(((1 1, 1.5 1, 2 1, 2 1.5,  2 2, 1.5 2,  1 2, 1 1.5, 1 1)))'::multipolygon, 0.5) as simplified,
                'multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon as original
          where name = 'Chili'""",
      "select-simplify-multigeometry.json")
  }

  test("geometry snap to grid") {
    compareSoqlResult(
      """select snap_to_grid('polygon((1 1, 1.5 1, 2 1, 2 1.5,  2 2, 1.5 2,  1 2, 1 1.5, 1 1))'::polygon, 2) as snapped
          where name = 'Chili'""",
      "select-snap-to-grid.json")
  }

  test("is empty") {
    val polygon = "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"
    val multipolygon = "MULTIPOLYGON(((1 1, 2 1, 2 2, 1 2, 1 1)))"
    val point = "POINT(10 40)"
    val multipoint = "MULTIPOINT((10 40), (40 30), (20 20), (30 10))"
    val emptyPoly = "MULTIPOLYGON EMPTY"
    val emptyPt = "MULTIPOINT EMPTY"
    // Inserting a null literal is tricky.
    val nullGeom = "simplify('POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))', 16)"
    compareSoqlResult(
      s"""
      select is_empty('$polygon') as poly,
        is_empty('$multipolygon') as mpoly,
        is_empty('$point') as point,
        is_empty('$multipoint') as mpoint,
        is_empty('$emptyPoly') as empty_poly,
        is_empty('$emptyPt') as empty_pt,
        is_empty($nullGeom) as null_geom
         where name = 'Chili'""",
      "select-is-empty.json")
  }

  test("visible at") {
    val polygon1   = """polygon((1 1, 2 1, 2 2, 1 2, 1 1))"""
    val polygon2   = """polygon((1 1, 3 1, 3 3, 1 3, 1 1))"""
    val polygon3   = """polygon((1 1, 4 1, 4 4, 1 4, 1 1))"""
    val point      = """point(10 40)"""
    val multipoint = """multipoint((10 40), (40 30), (20 20), (30 10))"""
    compareSoqlResult(
      s"""select visible_at('$polygon1', 2) as smaller,
                 visible_at('$polygon2', 2) as equal,
                 visible_at('$polygon3', 2) as bigger,
                 visible_at('$point', 2) as point,
                 visible_at('$multipoint', 2) as multipoint
          where name = 'Chili'""",
      "select-visible-at.json")
  }

  test("curated region test") {
    compareSoqlResult("select name, curated_region_test(multipolygon, 10) as test_result where name = 'Chili'", "select-curated-region-test.json")
  }

  test("curated region test too complex") {
    compareSoqlResult("select name, curated_region_test(multipolygon, 1) as test_result where name = 'Chili'", "select-curated-region-test-too-complex.json")
  }

  test("curated region test invalid geometry") {
    compareSoqlResult(
      """select name, curated_region_test('multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1),
        |                                                (1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon, 50) as test_result
        | where name = 'Chili'""".stripMargin,
      "select-curated-region-test-invalid-geometry.json")
  }

  test("curated region test contains null") {
    compareSoqlResult("select name, curated_region_test(multipolygon, 1) as test_result where code = 'FOO'", "select-curated-region-test-contains-null.json")
  }

  test("point.latitude") {
    compareSoqlResult(
      "select code, name, point.latitude, point.longitude where code = '14200' and point.latitude > 1 and point.longitude > 1",
      "select-point-lat-lng.json")
  }

  test("point to multipoint test") {
    compareSoqlResult("select name, geo_multi('point(1 1)'::point) as the_geom where name = 'Chili'" , "point-to-multipoint.json")
  }

  test("line to multiline test") {
    compareSoqlResult("select name, geo_multi('linestring(1 2, 3 4)'::line) as the_geom where name = 'Chili'", "line-to-multiline.json")
  }

  test("polygon to multipolygon test") {
    compareSoqlResult("select name, geo_multi('polygon((1 1, 2 1, 2 2, 1 2, 1 1))'::polygon) as the_geom where name = 'Chili'", "polygon-to-multipolygon.json")
  }

  test("multipoint to multipoint test") {
    compareSoqlResult("select name, geo_multi('multipoint(1 1, 2 2)'::multipoint) as the_geom where name = 'Chili'", "multipoint-to-multipoint.json")
  }

  test("multiline to multiline test") {
    compareSoqlResult("select name, geo_multi('multilinestring((1 2,3 4))'::multiline) as the_geom where name = 'Chili'", "multiline-to-multiline.json")
  }

  test("multipolygon to multipolygon test") {
    compareSoqlResult("""select name, geo_multi('multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1),
       | (1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon) as the_geom where name = 'Chili'""".stripMargin, "multipolygon-to-multipolygon.json")
  }


}
