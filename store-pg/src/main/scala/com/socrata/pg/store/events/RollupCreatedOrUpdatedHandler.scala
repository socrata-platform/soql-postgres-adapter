package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.secondary.{RollupInfo => SecRollupInfo}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}

/**
 * Handles creation or updates to rollup tables.  Note that this handles only the metadata, it doesn't
 * actually rebuild the rollups.  That is done separately at a higher level because they need rebuilding
 * when (almost) anything changes.
 */
case class RollupCreatedOrUpdatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo, secRollupInfo: SecRollupInfo) {
  pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName(secRollupInfo.name), secRollupInfo.soql)
}
