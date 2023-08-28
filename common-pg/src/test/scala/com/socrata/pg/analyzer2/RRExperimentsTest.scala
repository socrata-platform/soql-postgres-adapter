package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, MustMatchers}

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName

class RRExperimentsTest extends FunSuite with MustMatchers with SqlizerUniverse[SqlizerTest.TestMT] {
  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) =
    new MockTableFinder[SqlizerTest.TestMT](items.toMap)

  val analyzer = new SoQLAnalyzer[SqlizerTest.TestMT](TestTypeInfo, TestFunctionInfo)

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

    val expr = new RRExperiments[SqlizerTest.TestMT] with HasLabelProvider {
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
}
