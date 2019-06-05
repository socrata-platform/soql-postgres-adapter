package com.socrata.pg.store.events

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class SecondaryAddIndexHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                    datasetId: DatasetId,
                                    column: ColumnInfo[SoQLType],
                                    directives: JObject) {
  pgu.datasetMapWriter.createIndexDirectives(column, directives)
}
