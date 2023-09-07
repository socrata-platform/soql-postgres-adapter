package com.socrata.pg.analyzer2.rollup

import java.math.{BigDecimal => JBigDecimal}

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.{SoQLFunctions, Function, MonomorphicFunction, SoQLTypeInfo}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLNumber}

import com.socrata.pg.analyzer2.SqlizerUniverse

class SoQLSemigroupRewriter[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends SemigroupRewriter[MT]
{
  import SoQLTypeInfo.hasType

  private def countMerger(expr: Expr): Expr =
    FunctionCall[MT](
      MonomorphicFunction(
        SoQLFunctions.Coalesce,
        Map("a" -> SoQLNumber)
      ),
      Seq(
        FunctionCall[MT](
          MonomorphicFunction(
            SoQLFunctions.Sum,
            Map("a" -> SoQLNumber)
          ),
          Seq(expr)
        )(FuncallPositionInfo.None),
        LiteralValue[MT](SoQLNumber(JBigDecimal.ZERO))(AtomicPositionInfo.None)
      )
    )(FuncallPositionInfo.None)

  private def simple(f: Function[CT], binding: String): (Function[CT], Expr => Expr) =
    f -> { expr =>
      FunctionCall[MT](
        MonomorphicFunction(
          f,
          Map(binding -> expr.typ)
        ),
        Seq(expr)
      )(FuncallPositionInfo.None)
    }

  private val semigroupMap = Seq[(Function[CT], Expr => Expr)](
    simple(SoQLFunctions.Max, "a"),
    simple(SoQLFunctions.Min, "a"),
    simple(SoQLFunctions.Sum, "a"),
    SoQLFunctions.Count -> countMerger _,
    SoQLFunctions.CountStar -> countMerger _,
  ).map { case (func, rewriter) =>
      func.identity -> rewriter
  }.toMap


  override def apply(f: MonomorphicFunction): Option[Expr => Expr] =
    semigroupMap.get(f.function.identity)
}
