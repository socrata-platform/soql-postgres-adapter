package com.socrata.pg.server

import com.socrata.soql.stdlib.{Context, UserContext}
import com.socrata.soql.types.{SoQLText, SoQLBoolean, SoQLNumber, SoQLFloatingTimestamp, SoQLFixedTimestamp}
import java.math.{BigDecimal => JBigDecimal}

class SoQLContextTest extends SoQLTest {
  test("get_context(c)") {
    compareSoqlResult("select count(*) where make = get_context('make')", "where-get-context.json",
                      context = Context(
                        system = Map("make" -> "Skywalk",
                                     "model" -> "X-1500"),
                        user = UserContext.empty
                      ))
  }

  test("get_context(nonexistant)") {
    compareSoqlResult("select get_context('does not exist') as ctx limit 1", "context-nonexistant.json",
                      context = Context(
                        system = Map("make" -> "Skywalk",
                                     "model" -> "X-1500"),
                        user = UserContext.empty
                      ))
  }

  test("get_context(null)") {
    compareSoqlResult("select get_context(null) as ctx limit 1", "context-nonexistant.json")
  }

  test("bunch of context") {
    compareSoqlResult("select get_context('a') as a, get_context('b') as b, get_context('c'||'') as c, get_context('d') as d, get_context('e') as e, get_context('f') as f, get_context('g') as g limit 1", "context-many.json",
                      context = Context(
                        system = Map("a" -> "1",
                                     "b" -> "2",
                                     "c" -> "3",
                                     "d" -> "4",
                                     "e" -> "5",
                                     "f" -> "6",
                                     "g" -> "7"),
                        user = UserContext.empty
                      ))
  }
}
