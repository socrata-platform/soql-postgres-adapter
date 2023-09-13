package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.{SoQLValue, SoQLBoolean, SoQLType}
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctions, MonomorphicFunction}

trait RewritePassHelpers[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  def isLiteralTrue(e: Expr): Boolean
  def isOrderable(e: CT): Boolean
  def and: MonomorphicFunction
}

class SoQLRewritePassHelpers[MT <: MetaTypes with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue })] extends RewritePassHelpers[MT] {
  def isLiteralTrue(e: Expr): Boolean = {
    e match {
      case LiteralValue(SoQLBoolean(true)) => true
      case _ => false
    }
  }

  def isOrderable(e: SoQLType): Boolean = SoQLTypeInfo.isOrdered(e)

  val and = SoQLFunctions.And.monomorphic.get
}
