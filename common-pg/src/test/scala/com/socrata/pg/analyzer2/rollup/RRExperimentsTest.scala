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

  class TestRRExperiments(
    override val rollups: Seq[RollupInfo[MT]],
    override val labelProvider: LabelProvider
  ) extends RRExperiments[MT] with HasLabelProvider with RollupExact[MT] with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] {
    override val dtnOrdering = Ordering.String
    override def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = None
    override def funcallSubset(a: MonomorphicFunction,b: MonomorphicFunction): Option[MonomorphicFunction] = None
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
      Nil,
      analysis.labelProvider
    )

    val result = expr.rollup(analysis.statement).map(_.debugStr)
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

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] {
      override def funcallSubset(a: MonomorphicFunction,b: MonomorphicFunction) = None

      protected val labelProvider = analysis.labelProvider

      def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = None
    }

    val rollup = TestRollupInfo("rollup", tf, "select text, num * 5, num from @twocol where num = 5 order by text")

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

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] {
      val BottomByte = TestFunctions.BottomByte.monomorphic.get
      val BottomDWord = TestFunctions.BottomDWord.monomorphic.get

      override def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction) =
        (a, b) match {
          case (BottomByte, BottomDWord) =>
            Some(BottomByte)
          case _ =>
            None
        }

      protected val labelProvider = analysis.labelProvider

      def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = None
    }

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

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] {
      override def funcallSubset(a: MonomorphicFunction,b: MonomorphicFunction) = None

      protected val labelProvider = analysis.labelProvider

      val Sum = TestFunctions.Sum.monomorphic.get
      def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = {
        f match {
          case Sum =>
            Some { sum => AggregateFunctionCall[MT](Sum, Seq(sum), false, None)(FuncallPositionInfo.None) }
          case _ =>
            None
        }
      }
    }

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

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with SimpleFunctionSubset[MT] {
      val BottomByte = TestFunctions.BottomByte.monomorphic.get
      val BottomDWord = TestFunctions.BottomDWord.monomorphic.get

      override def funcallSubset(a: MonomorphicFunction, b: MonomorphicFunction) =
        (a, b) match {
          case (BottomByte, BottomDWord) =>
            Some(BottomByte)
          case _ =>
            None
        }

      protected val labelProvider = analysis.labelProvider

      val Max = TestFunctions.Max.monomorphic.get
      def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = {
        f match {
          case Max =>
            Some { max => AggregateFunctionCall[MT](Max, Seq(max), false, None)(FuncallPositionInfo.None) }
          case _ =>
            None
        }
      }
    }

    println(select.debugDoc)
    println(rollup.statement.debugDoc)
    val result = re.rollupSelectExact(select, rollup)

    println(result.map(_.debugDoc))
  }
}
