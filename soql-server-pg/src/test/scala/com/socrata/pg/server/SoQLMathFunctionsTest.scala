package com.socrata.pg.server

class SoQLMathFunctionsTest extends SoQLTest {

  test("signed_magnitude_10(x)") {
    compareSoqlResult(
      """select name, line_length, signed_magnitude_10(line_length),
                signed_magnitude_10(0) as z,
                signed_magnitude_10(9.9) as p,
                signed_magnitude_10(-9.8) as n
          order by name""",
      "select-signed_magnitude_10.json")
  }
}
