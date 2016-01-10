package com.socrata.pg.server


class SoQLNullFunctionsTest extends SoQLTest {

  test("is null") {
    compareSoqlResult("select name, make, size is null, v_max is null, available is null, certified is null, three_liner is null, spec is null, color is null where size is null and v_max is null and available is null and certified is null and three_liner is null and spec is null and color is null order by name", "where-null-isnull.json")
  }

  test("is not null") {
    compareSoqlResult("select name, make, size is not null, v_max is not null, available is not null, certified is not null, three_liner is not null, spec is not null, color is not null where size is not null and v_max is not null and available is not null and certified is not null and three_liner is not null and spec is not null and color is not null order by name", "where-null-isnotnull.json")
  }

  test("coalesce") {
    compareSoqlResult("select code, coalesce(country, name, 'x') as col where code = 'ATLAS'", "select-coalesce.json")
  }
}
