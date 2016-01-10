package com.socrata.pg.server

/**
 * Note that we do not run through soql merge in test but it does in real.
 */
class SoQLChainTest extends SoQLTest {

  test("chain basic") {
    compareSoqlResult(
      """SELECT code WHERE code = 'RUSH-3' |> SELECT code""".stripMargin,
      "chain-basic.json")
  }

  test("chain text") {
    compareSoqlResult(
      """SELECT code || 'a' as aa WHERE code = 'RUSH-3' and code != '00' |>
         SELECT aa || 'b' as bb WHERE aa != '11' |>
         SELECT bb || 'c' as cc WHERE bb !='22'""".stripMargin,
      "chain-text.json")
  }

  test("chain code text") {
    compareSoqlResult(
      """SELECT code, code || 'a' as aa WHERE code = 'RUSH-3' and code != '00' |>
         SELECT code, aa || 'b' as bb WHERE aa != '11' |>
         SELECT code, bb || 'c' as cc WHERE bb !='22'""".stripMargin,
      "chain-code-text.json")
  }

  test("chain code rename text") {
    compareSoqlResult(
      """SELECT code as xcode, code || 'a' as aa WHERE code = 'RUSH-3' and code != '00' |>
         SELECT xcode, aa || 'b' as bb WHERE aa != '11' |>
         SELECT xcode, bb || 'c' as cc WHERE bb !='22'""".stripMargin,
      "chain-code-rename-text.json")
  }

  test("chain location latitude") {
    compareSoqlResult(
      """SELECT code, location as ll, location.latitude as lat WHERE code = 'LOCATION' |>
         SELECT *""".stripMargin,
      "chain-location-lat.json")
  }

  test("chain location rename") {
    compareSoqlResult(
      """SELECT code, location WHERE code = 'LOCATION'|>
         SELECT code, location as loc1 |>
         SELECT code, loc1 as loc2""".stripMargin,
      "chain-location-rename.json")
  }

  test("chain inner group") {
    compareSoqlResult(
      "SELECT make, sum(v_min) WHERE make = 'OZONE' GROUP BY make |> SELECT *",
      "chain-inner-group.json")
  }

  test("chain outer group") {
    compareSoqlResult(
      "SELECT make, -v_min as neg_v_min WHERE make = 'OZONE' |> SELECT make, sum(neg_v_min) GROUP BY make",
      "chain-outer-group.json")
  }
}
