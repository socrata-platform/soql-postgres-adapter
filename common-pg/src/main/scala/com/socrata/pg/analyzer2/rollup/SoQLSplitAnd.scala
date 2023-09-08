package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.pg.analyzer2.SqlizerUniverse

class SoQLSplitAnd[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue})]
    extends SplitAnd[MT]
{
  private val And = SoQLFunctions.And.monomorphic.get

  override def split(e: Expr): Seq[Expr] =
    e match {
      case FunctionCall(And, args) =>
        args.flatMap(split)
      case other =>
        Seq(other)
    }

  override def merge(es: Seq[Expr]): Option[Expr] =
    if(es.isEmpty) {
      None
    } else {
      Some(es.reduceLeft { (acc, expr) => FunctionCall[MT](And, Seq(acc, expr))(FuncallPositionInfo.None) })
    }
}
