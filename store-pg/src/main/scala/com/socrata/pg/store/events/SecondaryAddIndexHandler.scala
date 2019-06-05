package com.socrata.pg.store.events

import com.rojoma.json.v3.ast.JObject
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo


case class IndexDirectiveCreatedOrUpdatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], column: ColumnInfo[SoQLType], directive: JObject) {
  pgu.datasetMapWriter.createOrUpdateIndexDirective(column, directive)
}
