package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions.{Function, MonomorphicFunction, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLText, SoQLBoolean, SoQLUrl}

class SoQLRewriteSearch[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})](override val searchBeforeQuery: Boolean) extends RewriteSearch[MT] {
  import SoQLTypeInfo.hasType

  override def litText(s: String): Expr = LiteralValue[MT](SoQLText(s))(AtomicPositionInfo.None)
  override def litBool(b: Boolean): Expr = LiteralValue[MT](SoQLBoolean(false))(AtomicPositionInfo.None)

  protected def isBoolean(t: SoQLType): Boolean = t == SoQLBoolean
  protected def isText(t: SoQLType): Boolean = t == SoQLText

  override def fieldsOf(expr: Expr): Seq[Expr] = {
    expr.typ match {
      case SoQLText =>
        Seq(expr)
      case SoQLUrl =>
        Seq(
          FunctionCall[MT](urlUrlExtractor, Seq(expr))(FuncallPositionInfo.None),
          FunctionCall[MT](urlDescriptionExtractor, Seq(expr))(FuncallPositionInfo.None)
        )
      case _ =>
        Nil
    }
  }

  override def mkAnd(left: Expr, right: Expr): Expr = {
    assert(left.typ == SoQLBoolean)
    assert(right.typ == SoQLBoolean)
    FunctionCall[MT](and, Seq(left, right))(FuncallPositionInfo.None)
  }

  override def mkOr(left: Expr, right: Expr): Expr = {
    assert(left.typ == SoQLBoolean)
    assert(right.typ == SoQLBoolean)
    FunctionCall[MT](or, Seq(left, right))(FuncallPositionInfo.None)
  }

  override def denull(string: Expr): Expr = {
    assert(string.typ == SoQLText)
    FunctionCall[MT](
      coalesce,
      Seq(string, litText(""))
    )(FuncallPositionInfo.None)
  }

  override val toTsVector = SoQLRewriteSearch.ToTsVector.monomorphic.get
  override val plainToTsQuery = SoQLRewriteSearch.PlainToTsQuery.monomorphic.get
  override val tsSearch = SoQLRewriteSearch.TsSearch.monomorphic.get
  override val concat = MonomorphicFunction(SoQLFunctions.Concat, Map("a" -> SoQLText, "b" -> SoQLText))

  private val coalesce = MonomorphicFunction(SoQLFunctions.Coalesce, Map("a" -> SoQLText))
  private val or = SoQLFunctions.Or.monomorphic.get
  private val and = SoQLFunctions.And.monomorphic.get
  private val urlUrlExtractor = SoQLFunctions.UrlToUrl.monomorphic.get
  private val urlDescriptionExtractor = SoQLFunctions.UrlToDescription.monomorphic.get
}

object SoQLRewriteSearch {
  private def mf(
    identity: String,
    name: FunctionName,
    params: Seq[SoQLType],
    varargs: Seq[SoQLType],
    result: SoQLType
  ) =
    new MonomorphicFunction(identity, name, params, varargs, result, isAggregate = false, needsWindow = false)(Function.Doc.empty).function

  // These result types are lies (they should be SoQLTSVector and
  // SoQLTSQuery respectively), but since users can't name these
  // functions it's ok-ish
  val ToTsVector = mf("to_tsvector", FunctionName("to_tsvector (unnameable)"), Seq(SoQLText), Nil, SoQLText)
  val PlainToTsQuery = mf("plainto_tsquery", FunctionName("plainto_tsquery (unnameable)"), Seq(SoQLText), Nil, SoQLText)

  val TsSearch = mf("tssearch", FunctionName("search (unnameable)"), Seq(SoQLText, SoQLText), Nil, SoQLBoolean)
}
