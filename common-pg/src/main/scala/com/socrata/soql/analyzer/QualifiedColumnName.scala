package com.socrata.soql.analyzer

import com.socrata.soql.environment.ColumnName

case class QualifiedColumnName(qualifier: Option[String], columnName: ColumnName)
