package com.socrata.pg.server

class SoQLCaseFunctionsTest extends SoQLTest {

  test("or inside case") {
    compareSoqlResult("""
      SELECT case(class = 'EN-A' or class = 'DHV-1', 'Beginner',
                  class = 'EN-B', 'Intermediate',
                  class = 'EN-C', 'Advanced',
                  true, 'Unknown') as category,
             class,
             name
       ORDER By category, name
       LIMIT 12""",
      "where-case-or.json")
  }

  test("two cases return the same value") {
    compareSoqlResult(
      """
        SELECT case(class = 'EN-A', 'Beginner',
                    class = 'DHV-1', 'Beginner',
                    class = 'EN-B', 'Intermediate',
                    class = 'EN-C', 'Advanced') as category,
               class,
               name
         ORDER By category, name
         LIMIT 12""",
      "where-case-or.json")
  }

  test("case group") {
    compareSoqlResult(
      """
        SELECT case(class = 'EN-A', 'Beginner',
                    class = 'DHV-1', 'Beginner',
                    class = 'EN-B', 'Intermediate',
                    class = 'EN-C', 'Advanced') as category,
               count(:id) as count
         GROUP By category
         ORDER BY category""",
      "group-case.json")
  }
}
