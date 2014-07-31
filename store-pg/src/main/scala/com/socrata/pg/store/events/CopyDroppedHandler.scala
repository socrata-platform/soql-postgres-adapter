package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}


case class CopyDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: TruthCopyInfo) {
  pgu.datasetMapWriter.dropCopy(copyInfo)
}
