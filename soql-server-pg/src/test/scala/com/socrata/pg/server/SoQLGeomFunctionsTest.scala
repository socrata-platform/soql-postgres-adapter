package com.socrata.pg.server

class SoQLGeomFunctionsTest extends SoQLTest {

  test("within_circle(point, lat, lon, m)") {
    compareSoqlResult("select make, name, point where within_circle(point, 32.50578, 34.8926, 10000) order by name", "where-within-circle.json")
  }

  test("within_circle lat lon reversed") {
    compareSoqlResult("select make, name, point where within_circle(point, 34.8926, 32.50578, 10000) order by name", "empty.json")
  }

  test("within_polygon matching apco") {
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
}
