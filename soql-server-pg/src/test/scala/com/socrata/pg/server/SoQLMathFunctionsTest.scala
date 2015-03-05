package com.socrata.pg.server

class SoQLMathFunctionsTest extends SoQLTest {

  test("magnitude(x,10)") {
    compareSoqlResult("select name, line_length, magnitude(line_length,10), magnitude(line_length,2) order by name", "select-magnitude.json")
  }
}
