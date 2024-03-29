package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.secondary.{RollupInfo => SecRollupInfo}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.pg.store.{PGSecondary, PGSecondaryUniverse, RollupManager}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

/**
 * Drops the rollup table and the metadata for the given copy.
 */
case class RollupDroppedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                copyInfo: CopyInfo,
                                secRollupInfo: SecRollupInfo) {
  val rollupName = new RollupName(secRollupInfo.name)
  val rm = new RollupManager(pgu, copyInfo)

  for {ri <- pgu.datasetMapReader.rollup(copyInfo, rollupName)} {
    pgu.datasetMapWriter.deleteRollupRelationships(ri)
    rm.dropRollup(ri, immediate = false)
    pgu.datasetMapWriter.dropRollup(copyInfo, Some(rollupName))
  }
}
