package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo

case class IndexDirectiveDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], column: ColumnInfo[SoQLType]) {
    pgu.datasetMapWriter.dropIndexDirective(column)
}
