package com.socrata.pg.server.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.{SoQLValue, SoQLBoolean, SoQLType}
import com.socrata.soql.functions.{SoQLTypeInfo, SoQLFunctions}

object RewritePasses {
  def isLiteralTrue[MT <: MetaTypes with ({ type ColumnValue = SoQLValue })](e: Expr[MT]): Boolean = {
    e match {
      case LiteralValue(SoQLBoolean(true)) => true
      case _ => false
    }
  }

  def isOrderable(e: SoQLType): Boolean = SoQLTypeInfo.isOrdered(e)

  val and = SoQLFunctions.And.monomorphic.get
}
