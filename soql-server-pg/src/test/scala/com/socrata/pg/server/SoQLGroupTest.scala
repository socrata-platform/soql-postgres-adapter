package com.socrata.pg.server

class SoQLGroupTest extends SoQLTest {

  test("group by text") {
    compareSoqlResult("select make, count(name) group by make order by make", "group-text.json")
  }

  test("group by text order by count") {
    compareSoqlResult("select make, count(name) group by make order by count(name) desc", "group-text-order-count.json")
  }

  test("group by text, numeric fn where having") {
    compareSoqlResult("select make, v_max > 50 as fast, count(name) where v_max is not null group by make, v_max > 50 having count(:id) > 1 order by v_max > 50 ", "group-text-expr-w-h.json")
  }

  test("group by text fn") {
    compareSoqlResult("select country || '''s ' || make as brand_made, count(name) group by country || '''s ' || make order by country || '''s ' || make", "group-text-expr.json")
  }
}
