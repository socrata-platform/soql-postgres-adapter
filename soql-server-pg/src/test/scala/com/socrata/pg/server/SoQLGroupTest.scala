package com.socrata.pg.server

import com.socrata.pg.soql.CaseInsensitive

class SoQLGroupTest extends SoQLTest {

  test("group by text") {
    compareSoqlResult("select upper(make) as umake, count(name) group by upper(make) order by upper(make)", "group-text.json")
  }

  test("group by text ci") {
    compareSoqlResult("select make as umake, count(name) group by make order by make", "group-text.json", caseSensitivity = CaseInsensitive)
  }


  test("group by text order by count") {
    compareSoqlResult("select upper(make) as umake, count(name) group by upper(make) order by count(name) desc, umake", "group-text-order-count.json")
  }

  test("group by text order by count ci") {
    compareSoqlResult("select make as umake, count(name) group by make order by count(name) desc, umake", "group-text-order-count.json", caseSensitivity = CaseInsensitive)
  }

  test("group by text, numeric fn where having") {
    compareSoqlResult("select make, v_max > 50 as fast, count(name) where v_max is not null group by make, v_max > 50 having count(:id) > 1 order by v_max > 50 ", "group-text-expr-w-h.json", caseSensitivity = CaseInsensitive)
  }

  test("group by text fn") {
    compareSoqlResult("select country || '''s ' || make as brand_made, count(name) group by country || '''s ' || make order by country || '''s ' || make", "group-text-expr.json", caseSensitivity = CaseInsensitive)
  }

  test("group by constant") {
    compareSoqlResult("select upper(make) as umake, 'one' as constant, count(name) as count_name group by upper(make), constant order by upper(make) |> select umake, count_name", "group-text.json")
  }
}
