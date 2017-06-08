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

  test("signed_magnitude_linear(x,width)") {
    compareSoqlResult(
      """select name, line_length, signed_magnitude_linear(line_length,10),
        |       signed_magnitude_linear(line_length,1) as unit,
        |       signed_magnitude_linear(0,1) as uz,
        |       signed_magnitude_linear(11.7,1) as up,
        |       signed_magnitude_linear(-2.3,1) as un,
        |       signed_magnitude_linear(0,10) as z,
        |       signed_magnitude_linear(9.9,10) as p,
        |       signed_magnitude_linear(-9.8,10) as n
        | order by name
      """.stripMargin,
    "select-signed_magnitude_linear.json")
  }

  ignore("median - requires a minimum of pg 9.4 for function percentile_disc") {
    compareSoqlResult(
      "select median(v_max) as m1, median(make) as m2",
      "median.json")
  }
}
