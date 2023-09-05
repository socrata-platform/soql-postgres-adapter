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

    val expr = new RRExperiments[MT] with HasLabelProvider {
      override val labelProvider = analysis.labelProvider
      implicit val dtnOrdering = Ordering.String

      override def rollupSelectExact(select: Select): Option[Statement] = {
        println("rollup select exect: " + select.debugDoc)
        None
      }

      override def rollupCombinedExact(combined: CombinedTables): Option[Statement] = {
        println("rollup combined exact: " + combined.debugDoc)
        None
      }
    }

    val result = expr.rollup(analysis.statement).map(_.debugStr)
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

    val Right(foundRollupTables) = tf.findTables(0, "select text, num * 5, num from @twocol where num = 5 order by text", Map.empty)
    val rollupAnalysis = analyzer(foundRollupTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val rollupSelect = rollupAnalysis.statement.asInstanceOf[Select]

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with FunctionSubset[MT] {
      def functionSubset(a: MonomorphicFunction,b: MonomorphicFunction): Boolean = false

      protected val labelProvider = analysis.labelProvider

      def mergeSemigroup(f: MonomorphicFunction): Option[Expr => Expr] = None
    }

    println(select.debugDoc)
    println(rollupSelect.debugDoc)
    val result = re.rollupSelectExact(select, rollupSelect, ScopedResourceName(0, ResourceName("rollup")), TableDescription.Dataset[MT](
      DatabaseTableName("rollup1"),
      CanonicalName("rollup1"),
      OrderedMap(
        DatabaseColumnName("c1") -> TableDescription.DatasetColumnInfo(ColumnName("text"), TestText, false),
        DatabaseColumnName("c2") -> TableDescription.DatasetColumnInfo(ColumnName("num_5"), TestNumber, false),
        DatabaseColumnName("c3") -> TableDescription.DatasetColumnInfo(ColumnName("num"), TestNumber, false),
      ),
      Nil,
      Nil
    ))

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

    val Right(foundRollupTables) = tf.findTables(0, "select text, num, sum(another_num) from @threecol group by text, num", Map.empty)
    val rollupAnalysis = analyzer(foundRollupTables, UserParameters.empty) match {
      case Right(a) => a
      case Left(e) => fail(e.toString)
    }
    val rollupSelect = rollupAnalysis.statement.asInstanceOf[Select]

    val re = new RollupExact[MT] with HasLabelProvider with SemigroupRewriter[MT] with FunctionSubset[MT] {
      def functionSubset(a: MonomorphicFunction,b: MonomorphicFunction): Boolean = false

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
    println(rollupSelect.debugDoc)
    val result = re.rollupSelectExact(select, rollupSelect, ScopedResourceName(0, ResourceName("rollup")), TableDescription.Dataset[MT](
      DatabaseTableName("rollup1"),
      CanonicalName("rollup1"),
      OrderedMap(
        DatabaseColumnName("c1") -> TableDescription.DatasetColumnInfo(ColumnName("text"), TestText, false),
        DatabaseColumnName("c2") -> TableDescription.DatasetColumnInfo(ColumnName("num"), TestNumber, false),
        DatabaseColumnName("c3") -> TableDescription.DatasetColumnInfo(ColumnName("sum_another_num"), TestNumber, false),
      ),
      Nil,
      Nil
    ))

    println(result.map(_.debugDoc))
  }
}
