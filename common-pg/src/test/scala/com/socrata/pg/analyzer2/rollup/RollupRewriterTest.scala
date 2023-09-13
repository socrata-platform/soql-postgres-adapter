package com.socrata.pg.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.collection.{OrderedMap, NonEmptySeq}
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.pg.analyzer2._

class RollupRewriterTest extends FunSuite with MustMatchers with RollupTestHelper with SqlizerUniverse[TestHelper.TestMT] {
  test("can produce multiple candidates if there are multiple rollups") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "english") -> D("num" -> TestNumber, "name" -> TestText),
      (0, "counts_by_name") -> Q(0, "twocol", "select text, @english.name, count(*) join @english on num = @english.num group by text, @english.name"),

      // These aren't actually real tables (which is why they're in a
      // different scope) but we need them to exist so we can
      // explicitly write soql that refers to them.
      (1, "rollup1") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber, "c4" -> TestText),
      (1, "rollup2") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestText, "c4" -> TestNumber).withPrimaryKey("c1","c2","c3")
    )

    val Right(foundTables) = tf.findTables(0, ResourceName("counts_by_name"))
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)

    val expr = new TestRollupRewriter(
      analysis.labelProvider,
      Seq(
        TestRollupInfo("rollup1", tf, "select @twocol.text as twocol_text, @twocol.num as twocol_num, @english.num as english_num, @english.name as english_name from @twocol join @english on @twocol.num = @english.num"),
        TestRollupInfo("rollup2", tf, "select text, num, @english.name, count(*) from @twocol join @english on num = @english.num group by text, num, @english.name")
      )
    )

    val result = expr.rollup(analysis.statement)
    result.length must be (2)

    locally {
      val Right(expectedRollup1FT) = tf.findTables(1, "select c1, c4, count(*) from @rollup1 group by c1, c4", Map.empty)
      val Right(expectedRollup1Analysis) = analyzer(expectedRollup1FT, UserParameters.empty)
      result(0) must be (isomorphicTo(expectedRollup1Analysis.statement))
    }

    locally {
      val Right(expectedRollup2FT) = tf.findTables(1, "select c1, c3, sum(c4) from @rollup2 group by c1, c3", Map.empty)
      val Right(expectedRollup2Analysis) = analyzer(expectedRollup2FT, UserParameters.empty)
      result(1) must be (isomorphicTo(expectedRollup2Analysis.statement))
    }
  }

  test("simple exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, num from @twocol where num = 5", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num * 5, num from @twocol where num = 5 order by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c3 from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple subset exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, bottom_byte(num) from @twocol where num = 5", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num * 5, bottom_dword(num) from @twocol where num = 5 order by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, bottom_byte(c3) from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple actual rollup") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "another_num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text, sum(another_num) from @threecol group by text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num, sum(another_num) from @threecol group by text, num")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3) from @rollup group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("function subset rollup") {
    val tf = tableFinder(
      (0, "twocol") -> D("num1" -> TestNumber, "num2" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestNumber, "c2" -> TestNumber).withPrimaryKey("c1")
    )

    val Right(foundTables) = tf.findTables(0, "select bottom_byte(num1), max(num2) from @twocol group by bottom_byte(num1)", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select bottom_dword(num1), max(num2) from @twocol group by bottom_dword(num1)")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select bottom_byte(c1), max(c2) from @rollup group by bottom_byte(c1)", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("simple rollup with additional AND in where") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select * from @twocol where num > 5 and num < 10", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select * from @twocol where num > 5")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c2 from @rollup where c2 < 10", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup avg - identical group by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestNumber, "c3" -> TestNumber).withPrimaryKey("c1")
    )

    val Right(foundTables) = tf.findTables(0, "select text, avg(num) from @twocol group by text", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, sum(num), count(num) from @twocol group by text")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, c2/c3 from @rollup", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }

  test("rollup avg - coarser group by") {
    val tf = tableFinder(
      (0, "twocol") -> D("text1" -> TestText, "text2" -> TestText, "num" -> TestNumber),
      (1, "rollup") -> D("c1" -> TestText, "c2" -> TestText, "c3" -> TestNumber, "c4" -> TestNumber).withPrimaryKey("c1", "c2")
    )

    val Right(foundTables) = tf.findTables(0, "select text1, avg(num) from @twocol group by text1", Map.empty)
    val Right(analysis) = analyzer(foundTables, UserParameters.empty)
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text1, text2, sum(num), count(num) from @twocol group by text1, text2")
    val Some(result) = TestRollupExact(select, rollup, analysis.labelProvider)

    locally {
      val Right(expectedRollupFT) = tf.findTables(1, "select c1, sum(c3)/sum(c4) from @rollup group by c1", Map.empty)
      val Right(expectedRollupAnalysis) = analyzer(expectedRollupFT, UserParameters.empty)
      result must be (isomorphicTo(expectedRollupAnalysis.statement))
    }
  }
}
