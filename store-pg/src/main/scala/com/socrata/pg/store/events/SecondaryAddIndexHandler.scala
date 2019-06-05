package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class SecondaryAddIndexHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                    datasetId: DatasetId,
                                    column: ColumnName,
                                    directives: String) {
  pgu.datasetMapWriter.createIndexDirectives(datasetId, column, directives)
}
