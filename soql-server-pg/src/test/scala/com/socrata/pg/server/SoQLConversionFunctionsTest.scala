package com.socrata.pg.server


class SoQLConversionFunctionsTest extends SoQLTest {

  test("num::text") {
    compareSoqlResult("select make, name, v_max::text where make = 'APCO' order by name", "where-conv-num2txt.json")
  }

  test("bool::text") {
    compareSoqlResult("select make, name, three_liner::text where make = 'APCO' order by name", "where-conv-bool2txt.json")
  }

  test("text::num") {
    compareSoqlResult("select name, make, code where code::number=14200 and name = 'karma'", "where-conv-txt2num.json")
  }

  test("text::bool") {
    compareSoqlResult("select make, name, three_liner where make = 'ozone' and '1'::boolean = three_liner or (make = 'apco' and '0'::boolean = three_liner) order by make, name", "where-conv-txt2bool.json")
  }

  test("text::floating_timestamp") { // ::floating_timestamp is optional if it is a string literal that match a specific pattern.
    compareSoqlResult("select name, make, available where available between '2008-08-01T15:00:00' and  '2008-08-01T16:00:00'::floating_timestamp order by available, name", "where-conv-txt2float_ts.json")
  }

  test("text::fixed_timestamp") { // ::fixed_timestamp is optional if it is a string literal that match a specific pattern.
    compareSoqlResult("select name, make, certified where certified between '2010-03-01' and '2010-03-01T18:11:23Z'::fixed_timestamp order by certified, name", "where-conv-txt2fix_ts.json")
  }
}
