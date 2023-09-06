package com.socrata.pg.analyzer2.rollup

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ResourceName, ColumnName}
import com.socrata.soql.functions.MonomorphicFunction

import com.socrata.pg.analyzer2._

class RRExperimentsTest extends FunSuite with MustMatchers with SqlizerUniverse[SqlizerTest.TestMT] {
  type MT = SqlizerTest.TestMT

  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) =
    new MockTableFinder[MT](items.toMap)

  val analyzer = new SoQLAnalyzer[MT](TestTypeInfo, TestFunctionInfo)

  def xtest(s: String)(f: => Any): Unit = {}

  class TestRollupExact(
    override val labelProvider: LabelProvider
  ) extends RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] with SplitAnd[MT] {
    private val Max = TestFunctions.Max.identity
    private val Sum = TestFunctions.Sum.identity
    private val Count = TestFunctions.Count.identity
    private val CountStar = TestFunctions.CountStar.identity
    override def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = {
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

    val BottomByte = TestFunctions.BottomByte.monomorphic.get
    val BottomDWord = TestFunctions.BottomDWord.monomorphic.get

    override def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction) =
      (a, b) match {
        case (BottomByte, BottomDWord) =>
          Some(BottomByte)
        case _ =>
          None
      }

    val And = TestFunctions.And.monomorphic.get
    override def splitAnd(e: Expr) =
      e match {
        case FunctionCall(And, args) =>
          args.flatMap(splitAnd)
        case other =>
          Seq(other)
      }
    override def mergeAnd(e: Seq[Expr]) = {
      if(e.isEmpty) {
        None
      } else {
        Some(e.reduceLeft { (acc, expr) => FunctionCall[MT](And, Seq(expr))(FuncallPositionInfo.None) })
      }
    }
  }

  class TestRRExperiments(
    override val rollups: Seq[RollupInfo[MT]],
    labelProvider: LabelProvider
  ) extends TestRollupExact(labelProvider) with RRExperiments[MT] {
    override val dtnOrdering = Ordering.String
  }

  test("huh") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber),
      (0, "english") -> D("num" -> TestNumber, "name" -> TestText),
      (0, "counts_by_name") -> Q(0, "twocol", "select text, @english.name, count(*) join @english on num = @english.num group by text, @english.name")
    )

    val Right(foundTables) = tf.findTables(0, ResourceName("counts_by_name"))
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }

    val expr = new TestRRExperiments(
      Seq(
        TestRollupInfo("rollup1", tf, "select @twocol.text as twocol_text, @twocol.num as twocol_num, @english.num as english_num, @english.name as english_name from @twocol join @english on @twocol.num = @english.num"),
        TestRollupInfo("rollup2", tf, "select text, num, @english.name, count(*) from @twocol join @english on num = @english.num group by text, num, @english.name")
      ),
      analysis.labelProvider
    )

    val result = expr.rollup(analysis.statement).map(_.debugStr)
    println(result)
  }

  class TestRollupInfo(
    val statement: Statement,
    val resourceName: types.ScopedResourceName[MT],
    val databaseName: DatabaseTableName
  ) extends RollupInfo[MT] {
    override def databaseColumnNameOfIndex(i: Int) = DatabaseColumnName(s"c$i")
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

  test("simple exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, num from @twocol where num = 5", Map.empty)
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num * 5, num from @twocol where num = 5 order by text")
    val re = new TestRollupExact(analysis.labelProvider)

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }

  test("simple subset exact") {
    val tf = tableFinder(
      (0, "twocol") -> D("text" -> TestText, "num" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, bottom_byte(num) from @twocol where num = 5", Map.empty)
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num * 5, bottom_dword(num) from @twocol where num = 5 order by text")
    val re = new TestRollupExact(analysis.labelProvider)

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }

  test("simple actual rollup") {
    val tf = tableFinder(
      (0, "threecol") -> D("text" -> TestText, "num" -> TestNumber, "another_num" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select text, sum(another_num) from @threecol group by text", Map.empty)
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select text, num, sum(another_num) from @threecol group by text, num")
    val re = new TestRollupExact(analysis.labelProvider)

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }

  test("function subset rollup") {
    val tf = tableFinder(
      (0, "twocol") -> D("num1" -> TestNumber, "num2" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select bottom_byte(num1), max(num2) from @twocol group by bottom_byte(num1)", Map.empty)
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select bottom_dword(num1), max(num2) from @twocol group by bottom_dword(num1)")
    val re = new TestRollupExact(analysis.labelProvider)

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }

  test("simple rollup with additional AND in where") {
    val tf = tableFinder(
      (0, "twocol") -> D("num" -> TestNumber, "num" -> TestNumber)
    )

    val Right(foundTables) = tf.findTables(0, "select * from @twocol where num > 5 and num < 10", Map.empty)
    val analysis = analyzer(foundTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val select = analysis.statement.asInstanceOf[Select]

    val rollup = TestRollupInfo("rollup", tf, "select * from @twocol where num > 5")
    val re = new TestRollupExact(analysis.labelProvider)

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }
}
