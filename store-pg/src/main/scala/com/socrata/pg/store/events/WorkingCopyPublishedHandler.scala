package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}

/**
 * Handles the WorkingCopyPublishedEvent
 */
case class WorkingCopyPublishedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo) {
  pgu.datasetMapWriter.publish(truthCopyInfo)
}
