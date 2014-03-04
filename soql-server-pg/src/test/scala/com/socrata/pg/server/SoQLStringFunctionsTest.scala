package com.socrata.pg.server


class SoQLStringFunctionsTest extends SoQLTest {

  test("select *") {
    compareSoqlResult("select make, name order by make, name", "result.json")
  }

  test("c < x") {
    compareSoqlResult("select make, name where name < 'buzz' order by name", "where-str-lt.json")
  }

  test("c <= x") {
    compareSoqlResult("select make, name where name <= 'buzz' order by name", "where-str-le.json")
  }

  test("c = x") {
    compareSoqlResult("select make, name where make = 'apco' order by name", "where-str-eq.json")
  }

  test("c != x") {
    compareSoqlResult("select make, name where make != 'apco' order by name", "where-str-ne.json")
  }

  test("c > x") {
    compareSoqlResult("select make, name where name > 'sprint evo' order by name", "where-str-gt.json")
  }

  test("c >= x") {
    compareSoqlResult("select make, name where name >= 'sprint evo' order by name", "where-str-ge.json")
  }

  // Demonstrate using lower in select.
  // TODO: Not sure if we want to use lower in where to suppress case insensitivity.
  test("lower(c)") {
    compareSoqlResult("select make, lower(name) where name = 'atlas'", "where-str-lower.json")
  }

  // Demonstrate using upper in select.
  // TODO: Not sure if we want to use upper in where to suppress case insensitivity.
  test("upper(c)") {
    compareSoqlResult("select make, upper(name) where name = 'atlas'", "where-str-upper.json")
  }

  test("starts_with(c, x)") {
    compareSoqlResult("select make, name where starts_with(make, 'Skyw') order by name", "where-str-starts_with.json")
  }

  test("contains(c, x)") {
    println("Search should be used instead of contains.")
  }
}
