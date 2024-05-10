package com.socrata.pg.analyzer2

sealed abstract class SoQLSqlizeAnnotation
object SoQLSqlizeAnnotation {
  case object Hidden extends SoQLSqlizeAnnotation
}
