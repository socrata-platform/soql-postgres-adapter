package com.socrata.pg.server


class SoQLStringFunctionsTest extends SoQLTest {

  test("select *") {
    compareSoqlResult("select make, name order by make, name", "result.json")
  }

  test("c < x") {
    compareSoqlResult("select make, name where name < 'Buzz' order by name", "where-str-lt.json")
  }

  test("c <= x") {
    compareSoqlResult("select make, name where name <= 'Buzz' order by name", "where-str-le.json")
  }

  test("c = x") {
    compareSoqlResult("select make, name where make = 'APCO' order by name", "where-str-eq.json")
  }

  test("c != x") {
    compareSoqlResult("select make, name where make != 'APCO' order by name", "where-str-ne.json")
  }

  test("c > x") {
    compareSoqlResult("select make, name where name > 'Sprint Evo' order by name", "where-str-gt.json")
  }

  test("c >= x") {
    compareSoqlResult("select make, name where name >= 'Sprint Evo' order by name", "where-str-ge.json")
  }

  test("lower(c)") {
    compareSoqlResult("select make, lower(name) where name = 'Atlas'", "where-str-lower.json")
  }

  test("upper(c)") {
    compareSoqlResult("select make, upper(name) where name = 'Atlas'", "where-str-upper.json")
  }

  test("starts_with(c, x)") {
    compareSoqlResult("select make, name where starts_with(make, 'Skyw') order by name", "where-str-starts_with.json")
  }

  test("contains(c, x)") {
    compareSoqlResult("select make, name where contains(name, 'arm') order by name", "where-str-contains.json")
  }
}
