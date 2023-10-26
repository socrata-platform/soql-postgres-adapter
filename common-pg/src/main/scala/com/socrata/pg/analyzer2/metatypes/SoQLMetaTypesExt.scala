package com.socrata.pg.analyzer2.metatypes

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.sqlizer.MetaTypesExt
import com.socrata.soql.types.{SoQLType, SoQLValue}

import com.socrata.pg.analyzer2._

trait SoQLMetaTypesExt extends MetaTypesExt { this: MetaTypes =>
  type ExtraContext = SoQLExtraContext
  type ExtraContextResult = SoQLExtraContext.Result
  type CustomSqlizeAnnotation = Nothing
  type SqlizerError = Nothing
}
