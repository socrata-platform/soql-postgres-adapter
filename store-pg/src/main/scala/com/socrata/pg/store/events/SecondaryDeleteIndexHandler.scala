package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}

case class SecondaryDeleteIndexHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                       datasetId: DatasetId,
                                       column: ColumnInfo[SoQLType]) {
  pgu.datasetMapWriter.deleteIndexDirectives(column)
}
