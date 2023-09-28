package com.socrata.pg.analyzer2

import org.scalatest.Assertions
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, Provenance}

import mocktablefinder._

object TestHelper {
  final class TestMT extends MetaTypes {
    type ResourceNameScope = Int
    type ColumnType = TestType
    type ColumnValue = TestValue
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }
}

trait TestHelper { this: Assertions =>
  type TestMT = TestHelper.TestMT

  def xtest(name: String)(test: => Any): Unit = {}

  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) =
    new MockTableFinder[TestMT](items.toMap)

  val analyzer = new SoQLAnalyzer[TestMT](TestTypeInfo, TestFunctionInfo, TestProvenanceMapper)

  val testTypeInfoProjection = TestTypeInfo.metaProject[TestMT]

  object TestProvenanceMapper extends types.ProvenanceMapper[TestMT] {
    def fromProvenance(prov: Provenance) = DatabaseTableName(prov.value)
    def toProvenance(dtn: types.DatabaseTableName[TestMT]) = Provenance(dtn.name)
  }

  class IsomorphicToMatcher[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) extends BeMatcher[Statement[MT]] {
    def apply(left: Statement[MT]) =
      MatchResult(
        left.isIsomorphic(right),
        left.debugStr + "\nwas not isomorphic to\n" + right.debugStr,
        left.debugStr + "\nwas isomorphic to\n" + right.debugStr
      )
  }

  def isomorphicTo[MT <: MetaTypes](right: Statement[MT])(implicit ev: HasDoc[MT#ColumnValue], ev2: HasDoc[MT#DatabaseTableNameImpl], ev3: HasDoc[MT#DatabaseColumnNameImpl]) = new IsomorphicToMatcher(right)
}
