package com.socrata.pg.server

class SoQLMathFunctionsTest extends SoQLTest {

  test("signed_magnitude_10(x)") {
    compareSoqlResult("select name, line_length, signed_magnitude_10(line_length) order by name",
                      "select-signed_magnitude_10.json")
  }
}
