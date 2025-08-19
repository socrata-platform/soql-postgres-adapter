package com.socrata.pg.analyzer2

import java.math.{BigDecimal => JBigDecimal}

import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.functions.{Function, MonomorphicFunction, FunctionType, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLText, SoQLBoolean, SoQLUrl, SoQLNumber}
import com.socrata.soql.sqlizer._

class SoQLRewriteSearch[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })](
  override val searchBeforeQuery: Boolean,
  dcnComparator: (types.DatabaseColumnName[MT], types.DatabaseColumnName[MT]) => Boolean
) extends RewriteSearch[MT] {
  import SoQLTypeInfo.hasType

  override def compareDatabseColumnNames(a: DatabaseColumnName, b: DatabaseColumnName) = dcnComparator(a, b)

  type NumberLiteral = JBigDecimal
  override def litText(s: String): Expr = LiteralValue[MT](SoQLText(s))(AtomicPositionInfo.Synthetic)
  override def litBool(b: Boolean): Expr = LiteralValue[MT](SoQLBoolean(false))(AtomicPositionInfo.Synthetic)
  override def litNum(s: String): Option[NumberLiteral] =
    try {
      Some(new JBigDecimal(s))
    } catch {
      case e: NumberFormatException =>
        None
    }

  protected def isBoolean(t: SoQLType): Boolean = t == SoQLBoolean
  protected def isText(t: SoQLType): Boolean = t == SoQLText

  override def textFieldsOf(expr: Expr): Seq[Expr] = {
    expr.typ match {
      case SoQLText =>
        Seq(expr)
      case SoQLUrl =>
        // They must be in this order because we need them in physical column order
        Seq(
          FunctionCall[MT](urlDescriptionExtractor, Seq(expr))(FuncallPositionInfo.Synthetic),
          FunctionCall[MT](urlUrlExtractor, Seq(expr))(FuncallPositionInfo.Synthetic)
        )
      case _ =>
        Nil
    }
  }

  override def numberFieldsOf(expr: Expr): Seq[Expr] = {
    expr.typ match {
      case SoQLNumber =>
        Seq(expr)
      case _ =>
        Nil
    }
  }

  override def mkAnd(left: Expr, right: Expr): Expr = {
    assert(left.typ == SoQLBoolean)
    assert(right.typ == SoQLBoolean)
    FunctionCall[MT](and, Seq(left, right))(FuncallPositionInfo.Synthetic)
  }

  override def mkOr(left: Expr, right: Expr): Expr = {
    assert(left.typ == SoQLBoolean)
    assert(right.typ == SoQLBoolean)
    FunctionCall[MT](or, Seq(left, right))(FuncallPositionInfo.Synthetic)
  }

  override def mkEq(left: Expr, right: JBigDecimal): Expr = {
    assert(left.typ == SoQLNumber)
    FunctionCall[MT](
      MonomorphicFunction(
        eq,
        Map("a" -> SoQLNumber)
      ),
      Seq(left, LiteralValue[MT](SoQLNumber(right))(AtomicPositionInfo.Synthetic))
    )(FuncallPositionInfo.Synthetic)
  }

  override def denull(string: Expr): Expr = {
    assert(string.typ == SoQLText)
    FunctionCall[MT](
      coalesce,
      Seq(string, litText(""))
    )(FuncallPositionInfo.Synthetic)
  }

  override def searchTerm(schema: Iterable[(ColumnLabel, Rep[MT])]): Option[Doc[Nothing]] = {
    val term =
      schema.toSeq.flatMap { case (label, rep) =>
        rep.typ match {
          case SoQLText | SoQLUrl =>
            rep.expandedDatabaseColumns(label)
          case _ =>
            Nil
        }
      }.map { col =>
        Seq(col, d"''").funcall(d"coalesce")
      }

    if(term.nonEmpty) {
      Some(term.concatWith { (a: Doc[Nothing], b: Doc[Nothing]) => a +#+ d"|| ' ' ||" +#+ b })
    } else {
      None
    }
  }

  override val toTsVector = SoQLRewriteSearch.ToTsVector.monomorphic.get
  override val plainToTsQuery = SoQLRewriteSearch.PlainToTsQuery.monomorphic.get
  override val tsSearch = SoQLRewriteSearch.TsSearch.monomorphic.get
  override val concat = MonomorphicFunction(SoQLFunctions.Concat, Map("a" -> SoQLText, "b" -> SoQLText))

  private val coalesce = MonomorphicFunction(SoQLFunctions.Coalesce, Map("a" -> SoQLText))
  private val or = SoQLFunctions.Or.monomorphic.get
  private val and = SoQLFunctions.And.monomorphic.get
  private val eq = SoQLFunctions.Eq
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
    new MonomorphicFunction(identity, name, params, varargs, result, FunctionType.Normal)(Function.Doc.empty).function

  // These result types are lies (they should be SoQLTSVector and
  // SoQLTSQuery respectively), but since users can't name these
  // functions it's ok-ish
  val ToTsVector = mf("to_tsvector", FunctionName("to_tsvector (unnameable)"), Seq(SoQLText), Nil, SoQLText)
  val PlainToTsQuery = mf("plainto_tsquery", FunctionName("plainto_tsquery (unnameable)"), Seq(SoQLText), Nil, SoQLText)

  val TsSearch = mf("tssearch", FunctionName("search (unnameable)"), Seq(SoQLText, SoQLText), Nil, SoQLBoolean)

  def simpleDcnComparator[MT <: MetaTypes](a: types.DatabaseColumnName[MT], b: types.DatabaseColumnName[MT])(implicit ev: Ordering[MT#DatabaseColumnNameImpl]): Boolean = {
    val DatabaseColumnName(aVal) = a
    val DatabaseColumnName(bVal) = b
    ev.lt(aVal, bVal)
  }
}
