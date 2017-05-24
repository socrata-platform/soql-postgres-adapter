package com.socrata.soql.analyzer

import com.socrata.soql.environment.ColumnName

case class QualifiedColumnName(val qualifier: Option[String], val columnName: ColumnName)
