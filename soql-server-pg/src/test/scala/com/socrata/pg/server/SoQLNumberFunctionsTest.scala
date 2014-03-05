package com.socrata.pg.server


class SoQLNumberFunctionsTest extends SoQLTest {

  test("c < x") {
    compareSoqlResult("select make, name, v_max where v_max < 50 order by v_max, name", "where-num-lt.json")
  }

  test("c <= x") {
    compareSoqlResult("select make, name, v_max where v_max <= 50 order by v_max, name", "where-num-le.json")
  }

  test("c = x") {
    compareSoqlResult("select make, name, v_max where v_max = 50 order by v_max, name", "where-num-eq.json")
  }

  test("c != x") {
    compareSoqlResult("select make, name, v_max where v_max != 50 order by v_max, name", "where-num-ne.json")
  }

  test("c > x") {
    compareSoqlResult("select make, name, v_max where v_max > 50 order by v_max, name", "where-num-gt.json")
  }

  test("c >= x") {
    compareSoqlResult("select make, name, v_max where v_max >= 50 order by v_max, name", "where-num-ge.json")
  }

  test("c + x") {
    compareSoqlResult("select make, name, v_max where v_max + 50 = 100 order by v_max, name", "where-num-add.json")
  }

  test("c - x") {
    compareSoqlResult("select make, name, v_max where v_max - 50 = 0 order by v_max, name", "where-num-sub.json")
  }

  test("+c") {
    compareSoqlResult("select make, name, v_max where +v_max = +50 order by v_max, name", "where-num-plus.json")
  }

  test("-c") {
    compareSoqlResult("select make, name, v_max where -v_max = -50 order by v_max, name", "where-num-neg.json")
  }

  test("c * x") {
    compareSoqlResult("select make, name, v_max where v_max*2 = 100 order by v_max, name", "where-num-mul.json")
  }

  test("c / x") {
    compareSoqlResult("select make, name, v_max where v_max/2 = 25 order by v_max, name", "where-num-div.json")
  }

  test("c ^ x and ^'s precedence is higher than *'s") {
    compareSoqlResult("select make, name, v_max, 10 * 2 ^ 3 = 10 * (2 ^ 3) as t1, 10 * 2 ^ 3 = 80 as t2 where v_max ^ 2 = 2500 order by name", "where-num-exp.json")
  }

  test("c % x") {
    compareSoqlResult("select make, name, v_max where v_max % 10 = 2 order by name", "where-num-mod.json")
  }
}
