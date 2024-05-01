package com.socrata.pg.server

import com.socrata.soql.exceptions.BadParse


class SoQLJoinTest extends SoQLTest {

  test("plain") {
    compareSoqlResult(s"""
SELECT make, name, @manufacturer_${uniquifier}.timezone
  JOIN @manufacturer_${uniquifier} on make=@manufacturer_${uniquifier}.make
 ORDER by make, code
                      """,
                      "join.json",
                      joinDatasetCtx = plainCtx)

  }

  test("alias") {
    compareSoqlResult(s"""
SELECT make, name, @m.timezone
  JOIN @manufacturer_${uniquifier} as m on make=@m.make
 WHERE @m.make='OZONE'
 ORDER by @m.make, code
                      """,
                      "join-where.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join a table twice") {
    compareSoqlResult(s"""
SELECT make, name, @m2.timezone
  JOIN @manufacturer_${uniquifier} as m on make=@m.make
  JOIN @manufacturer_${uniquifier} as m2 on make=@m2.make
 WHERE @m.make='OZONE'
 ORDER by @m.make, code
                      """,
                      "join-where.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("chain count") {
    compareSoqlResult(s"""
SELECT make, name, @m.timezone
  JOIN @manufacturer_${uniquifier} as m on make=@m.make
 WHERE @m.make='OZONE'
    |>
SELECT count(*)
                      """,
                      "join-chain-count.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("group and then join") {
    compareSoqlResult(s"""
SELECT make, count(name) as ct
 GROUP by make
    |>
SELECT make, ct, @m.timezone
  JOIN @manufacturer_${uniquifier} as m on make=@m.make
 WHERE @m.make='OZONE'
                      """,
                      "group-join.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join with sub-query") {
    compareSoqlResult(s"""
SELECT make, code, @m.timezone
  JOIN (SELECT make, timezone FROM @manufacturer_${uniquifier} WHERE make='APCO') as m on make=@m.make
 WHERE @m.make='APCO'
 ORDER by @m.make, code
                      """,
                      "join-subquery.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join with chained sub-query") {
    compareSoqlResult(s"""
SELECT make, code, @m.timezone
 JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO'
           |>
       SELECT make, timezone) as m on make=@m.make
 WHERE @m.make='APCO'
 ORDER by @m.make, code
                      """,
                      "join-subquery.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join with grouping sub-query") {
    compareSoqlResult(s"""
SELECT make, code, @m.maxtimezone
  JOIN (SELECT make, max(timezone) as maxtimezone FROM @manufacturer_${uniquifier} WHERE make='APCO' GROUP by make) as m on make=@m.make
 WHERE @m.make='APCO'
 ORDER by @m.make, code
                      """,
                      "join-group-subquery.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join with chained grouping sub-query") {
    compareSoqlResult(s"""
SELECT make, code, @m.maxtimezone
  JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO'
           |>
        SELECT make, max(timezone) as maxtimezone GROUP by make) as m on make=@m.make
 WHERE @m.make='APCO'
 ORDER by @m.make, code
                      """,
                      "join-group-subquery.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("chain join with chained grouping sub-query") {
    compareSoqlResult(s"""
SELECT make, code, @m.maxtimezone
  JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO' |> SELECT make, max(timezone) as maxtimezone GROUP by make) as m on make=@m.make
 WHERE @m.make='APCO' order by @m.make, code
    |>
SELECT make, count(maxtimezone) GROUP BY make
                      """,
                      "join-group-subquery-chain.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join two tables") {
    compareSoqlResult(s"""
         SELECT make, code, @m.timezone, @c.description as classification
           JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO' |> SELECT make, timezone) as m on make=@m.make
LEFT OUTER JOIN @classification_${uniquifier} as c on class=@c.id
          WHERE @m.make='APCO'
          ORDER by @m.make, code
                      """,
                      "join-2tables.json",
                      joinDatasetCtx = aliasCtx)
  }

  test("join and search") {
    val soql = s"""
         SELECT make, code, @m.timezone, @c.description as classification
           JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO' |> SELECT make, timezone) as m on make=@m.make
LEFT OUTER JOIN @classification_${uniquifier} as c on class=@c.id
          WHERE @m.make='APCO'
         SEARCH 'beginner'
          ORDER by @m.make, code
               """
    compareSoqlResult(soql, "join-search.json", joinDatasetCtx = aliasCtx, leadingSearch = false)
    compareSoqlResult(soql, "empty.json", joinDatasetCtx = aliasCtx, leadingSearch = true)
  }

  test("invalid table alias") {
    intercept[BadParse] {
      compareSoqlResult(s"""
SELECT make, code, @z$$.timezone
  JOIN (SELECT make, timezone FROM @manufacturer_${uniquifier} WHERE make='APCO') as z$$ on make=@z$$.make
 WHERE @z$$.make='APCO'
 ORDER by @z$$.make, code
                        """,
                        "join-subquery.json",
                        joinDatasetCtx =
                        aliasCtx)
      }
  }

  test("nested join") {
    compareSoqlResult(s"""
         SELECT make, code, @m.timezone, @m.continent, @c.description as classification
           JOIN (SELECT * FROM @manufacturer_${uniquifier} WHERE make='APCO'
                     |> SELECT make, timezone, @co.continent
                          JOIN @country_${uniquifier} as co on country=@co.country) as m on make=@m.make
LEFT OUTER JOIN @classification_${uniquifier} as c on class=@c.id
          WHERE @m.make='APCO'
            and @m.continent = 'Asia'
          ORDER by @m.make, code
                      """,
      "join-nested.json",
      joinDatasetCtx = aliasCtx)
  }
}
