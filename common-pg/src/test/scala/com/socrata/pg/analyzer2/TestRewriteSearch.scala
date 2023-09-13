package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.MonomorphicFunction

object TestRewriteSearch extends RewriteSearch[TestHelper.TestMT] {
  type TestMT = TestHelper.TestMT
  import TestTypeInfo.hasType

  override val searchBeforeQuery = true

  override def concat = TestFunctions.Concat.monomorphic.get

  override def denull(string: Expr) =
    FunctionCall[TestMT](coalesce, Seq(string, litText("")))(FuncallPositionInfo.None)

  override def fieldsOf(expr: Expr): Seq[Expr] =
    expr.typ match {
      case TestText => Seq(expr)
      case _ => Nil
    }

  override def litBool(b: Boolean): Expr = LiteralValue[TestMT](TestBoolean(b))(AtomicPositionInfo.None)
  override def litText(s: String): Expr = LiteralValue[TestMT](TestText(s))(AtomicPositionInfo.None)

  override def isText(t: TestType) = t == TestText
  override def isBoolean(t: TestType) = t == TestBoolean

  override def mkAnd(left: Expr,right: Expr): Expr =
    FunctionCall[TestMT](
      TestFunctions.And.monomorphic.get,
      Seq(left, right)
    )(FuncallPositionInfo.None)

  override def mkOr(left: Expr,right: Expr): Expr =
    FunctionCall[TestMT](
      TestFunctions.Or.monomorphic.get,
      Seq(left, right)
    )(FuncallPositionInfo.None)

  override def plainToTsQuery: MonomorphicFunction = ???
  override def toTsVector: MonomorphicFunction = ???
  override def tsSearch: MonomorphicFunction = ???

  private val coalesce = MonomorphicFunction(TestFunctions.Coalesce, Map("a" -> TestText))
}
