package com.socrata.pg.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.collection.{OrderedMap, NonEmptySeq}
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.pg.analyzer2._

class RollupRewriterTest extends FunSuite with MustMatchers with SqlizerUniverse[SqlizerTest.TestMT] {
  type MT = SqlizerTest.TestMT

  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) =
    new MockTableFinder[MT](items.toMap)

  val analyzer = new SoQLAnalyzer[MT](TestTypeInfo, TestFunctionInfo, TestProvenanceMapper)

  object TestSemigroupRewriter extends SemigroupRewriter[MT] {
    private val Max = TestFunctions.Max.identity
    private val Sum = TestFunctions.Sum.identity
    private val Count = TestFunctions.Count.identity
    private val CountStar = TestFunctions.CountStar.identity

    override def apply(f: MonomorphicFunction): Option[Expr => Expr] = {
      f.function.identity match {
        case Max =>
          Some { max => AggregateFunctionCall[MT](TestFunctions.Max.monomorphic.get, Seq(max), false, None)(FuncallPositionInfo.None) }
        case Sum =>
          Some { sum => AggregateFunctionCall[MT](TestFunctions.Sum.monomorphic.get, Seq(sum), false, None)(FuncallPositionInfo.None) }
        case Count | CountStar =>
          Some { count => AggregateFunctionCall[MT](TestFunctions.Sum.monomorphic.get, Seq(count), false, None)(FuncallPositionInfo.None) }
        case _ =>
          None
      }
    }
  }

  object TestFunctionSubset extends SimpleFunctionSubset[MT] {
    val BottomByte = TestFunctions.BottomByte.monomorphic.get
    val BottomDWord = TestFunctions.BottomDWord.monomorphic.get

    override def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction) =
      (a, b) match {
        case (BottomByte, BottomDWord) =>
          Some(BottomByte)
        case _ =>
          None
      }
  }

  object TestFunctionSplitter extends FunctionSplitter[MT] {
    private val avg = (
      TestFunctions.Div.monomorphic.get,
      Seq(
        TestFunctions.Sum.monomorphic.get,
        MonomorphicFunction(TestFunctions.Count, Map("a" -> TestNumber))
      )
    )

    override def apply(f: MonomorphicFunction): Option[(MonomorphicFunction, Seq[MonomorphicFunction])] =
      if(f.function.identity == TestFunctions.Avg.identity) {
        Some(avg)
      } else {
        None
      }
  }

  object TestSplitAnd extends SplitAnd[MT] {
    val And = TestFunctions.And.monomorphic.get
    override def split(e: Expr) =
      e match {
        case FunctionCall(And, args) =>
          val nonEmptyArgs = NonEmptySeq.fromSeq(args).getOrElse {
            throw new Exception("AND with no arguments??")
          }
          nonEmptyArgs.flatMap(split)
        case other =>
          NonEmptySeq(other)
      }
    override def merge(e: NonEmptySeq[Expr]) = {
      e.foldLeft1(identity) { (acc, expr) => FunctionCall[MT](And, Seq(acc, expr))(FuncallPositionInfo.None) }
    }
  }

  object TestRollupExact extends RollupExact[MT](
    TestSemigroupRewriter,
    TestFunctionSubset,
    TestFunctionSplitter,
    TestSplitAnd,
    Stringifier.pretty
  )

  class TestRollupInfo(
    val statement: Statement,
    val resourceName: types.ScopedResourceName[MT],
    val databaseName: DatabaseTableName
  ) extends RollupInfo[MT] {
    override def databaseColumnNameOfIndex(i: Int) = DatabaseColumnName(s"c${i+1}")
  }

  object TestRollupInfo {
    def apply(name: String, tf: TableFinder[MT], soql: String): TestRollupInfo = {
      val Right(foundTables) = tf.findTables(0, soql, Map.empty)
      val analysis = analyzer(foundTables, UserParameters.empty) match {
        case Right(a) => a
        case Left(e) => fail(e.toString)
      }
      new TestRollupInfo(analysis.statement, ScopedResourceName(0, ResourceName(name)), DatabaseTableName(name))
    }
  }

  class TestRollupRewriter(
    labelProvider: LabelProvider,
    rollups: Seq[RollupInfo[MT]]
  ) extends RollupRewriter(labelProvider, TestRollupExact, rollups)

  class IsomorphicToMatcher[MT <: MetaTypes](right: Statement)(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) extends BeMatcher[Statement] {
    def apply(left: Statement) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo(right: Statement) = new IsomorphicToMatcher[MT](right)

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
