package com.socrata.pg.analyzer2

import org.scalatest.Assertions
import org.scalatest.matchers.{BeMatcher, MatchResult}

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.{ColumnName, ResourceName, HoleName, Provenance}
import com.socrata.soql.sqlizer._

import mocktablefinder._

object TestHelper {
  final class TestMT extends MetaTypes with metatypes.SoQLMetaTypesExt {
    type ResourceNameScope = Int
    type ColumnType = TestType
    type ColumnValue = TestValue
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  object TestProvenanceMapper extends types.ProvenanceMapper[TestMT] {
    def fromProvenance(prov: Provenance) = DatabaseTableName(prov.value)
    def toProvenance(dtn: types.DatabaseTableName[TestMT]) = Provenance(dtn.name)
  }

  object TestExprSqlFactory extends ExprSqlFactory[TestMT] {
    def compress(expr: Option[Expr], rawSqls: Seq[Doc]): Doc =
      expr match {
        case Some(_ : NullLiteral) =>
          d"null :: jsonb"
        case _ =>
          rawSqls.funcall(d"soql_compress_compound")
      }
  }
}

trait TestHelper { this: Assertions =>
  type TestMT = TestHelper.TestMT

  def xtest(name: String)(test: => Any): Unit = {}

  def tableFinder(items: ((Int, String), Thing[Int, TestType])*) =
    new MockTableFinder[TestMT](items.toMap)

  val TestProvenanceMapper = TestHelper.TestProvenanceMapper
  val TestExprSqlFactory = TestHelper.TestExprSqlFactory

  class TestSqlNamespaces extends SqlNamespaces[TestMT] {
    override def databaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      Doc(name)
    }

    override def databaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(name) = dcn
      Doc(name)
    }

    protected override def gensymPrefix: String = "g"
    protected override def idxPrefix: String ="idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
  }

  val analyzer = new SoQLAnalyzer[TestMT](TestTypeInfo, TestFunctionInfo, TestProvenanceMapper)

  val testTypeInfoProjection = TestTypeInfo.metaProject[TestMT]

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
