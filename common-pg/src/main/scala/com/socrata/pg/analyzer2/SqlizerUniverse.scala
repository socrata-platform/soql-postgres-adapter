package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.prettyprint

import com.socrata.pg.analyzer2

trait SqlizerUniverse[MT <: MetaTypes] extends StatementUniverse[MT] {
  type AugmentedType = analyzer2.AugmentedType[MT]
  type Rep = analyzer2.Rep[MT]
  type ExprSql = analyzer2.ExprSql[MT]
  type OrderBySql = analyzer2.OrderBySql[MT]
  type ExprSqlizer = analyzer2.ExprSqlizer[MT]
  type FuncallSqlizer = analyzer2.FuncallSqlizer[MT]
  type Sqlizer = analyzer2.Sqlizer[MT]
  type SqlizeAnnotation = analyzer2.SqlizeAnnotation[MT]
  type Doc = prettyprint.Doc[SqlizeAnnotation]
  type DocNothing = prettyprint.Doc[Nothing]
  type SqlNamespaces = analyzer2.SqlNamespaces[MT]
  type ResultExtractor = analyzer2.ResultExtractor[MT]
  type RewriteSearch = analyzer2.RewriteSearch[MT]
  type ProvenanceTracker = analyzer2.ProvenanceTracker[MT]
}
