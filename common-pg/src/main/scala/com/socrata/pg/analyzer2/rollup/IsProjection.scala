package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

trait IsProjection[MT <: MetaTypes] extends SqlizerUniverse[MT] { this: ExprIsomorphism[MT] =>
  def isProjection(expr: Expr, candidates: Seq[(Expr, Expr)]): Option[Expr] = {
    expr match {
      case lit: LiteralValue =>
        Some(lit)
      case nul: NullLiteral =>
        Some(nul)
      case other =>
        for((candidate, columnRef) <- candidates) {
          if(isIsomorphicTo(expr, candidate)) {
            return Some(columnRef)
          } else {
            expr match {
              case fc@FunctionCall(func, args) =>
                val break = new scala.util.control.Breaks
                break.breakable {
                  val rewrittenArgs = args.map { arg =>
                    isProjection(arg, candidates) match {
                      case Some(columnRef) =>
                        columnRef
                      case None =>
                        break.break()
                    }
                  }
                  return Some(FunctionCall(func, rewrittenArgs)(fc.position))
                }
              case _ =>
                // nope
            }
          }
        }
        None
    }
  }
}
