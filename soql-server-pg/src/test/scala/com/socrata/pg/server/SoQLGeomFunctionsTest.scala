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

  test("point conversion") {
    compareSoqlResult("select name, 'point (1.1 2.2)'::point as northeast, 'pOInt(-1.1 -2.2)'::point as southwest where name = 'Chili'", "select-point-lit.json")
  }

  test("multipolygon conversion") {
    compareSoqlResult("select name, 'multipolygon(((1 1, 2 1, 2 2, 1 2, 1 1)))'::multipolygon as multipolygon where name = 'Chili'", "select-mpolygon-lit.json")
  }

  test("multiline conversion") {
    compareSoqlResult("select name, 'multilinestring((10.123456 -20.123456, -30.654321 40.654321))'::multiline as multiline where name = 'Chili'", "select-mline-lit.json")
  }
}