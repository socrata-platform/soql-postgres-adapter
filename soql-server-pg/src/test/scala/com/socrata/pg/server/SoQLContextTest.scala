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
    compareSoqlResult("select get_context('a') as a, get_context('b') as b, get_context('c') as c, get_context('d') as d, get_context('e') as e, get_context('f') as f, get_context('g') as g limit 1", "context-many.json",
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

  test("parameters") {
    val SoQLFloatingTimestamp.StringRep(d1) = "2001-01-01T02:03:44.543"
    val SoQLFixedTimestamp.StringRep(d2) = "2001-07-04T13:22:11.003-0700"

    compareSoqlResult("""select
                           text_parameter('text_1') as a, text_parameter('text_2') as b,
                           number_parameter('n1') as c, number_parameter('n2') as d,
                           boolean_parameter('b') as e,
                           floating_timestamp_parameter('d') as f,
                           fixed_timestamp_parameter('d2') as g,
                           text_parameter('n1') as wrong_type
                         limit 1""",
                      "parameters.json",
                      context = Context(
                        system = Map.empty,
                        user = UserContext(text = Map("text_1" -> SoQLText("hello"),
                                                      "text_2" -> SoQLText("world")),
                                           num = Map("n1" -> SoQLNumber(JBigDecimal.valueOf(3)),
                                                     "n2" -> SoQLNumber(JBigDecimal.valueOf(42))),
                                           bool = Map("b" -> SoQLBoolean(true)),
                                           floating = Map("d" -> SoQLFloatingTimestamp(d1)),
                                           fixed = Map("d2" -> SoQLFixedTimestamp(d2)))
                      ))
  }
}
