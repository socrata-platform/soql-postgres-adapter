package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._

final class RedshiftExprSqlFactory[MT <: MetaTypes with metatypes.SoQLMetaTypesExt] extends ExprSqlFactory[MT] {
  override def compress(expr: Option[Expr], rawSqls: Seq[Doc]): Doc =
    expr match {
      case Some(_ : NullLiteral) =>
        d"null :: jsonb"
      case _ =>
        rawSqls.funcall(d"soql_compress_compound")
    }
}
