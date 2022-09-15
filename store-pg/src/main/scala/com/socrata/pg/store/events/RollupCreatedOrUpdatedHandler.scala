package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.secondary.{RollupInfo => SecRollupInfo}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.pg.store.RollupManager.parseAndCollectTableNames
import com.socrata.pg.store.{LocalRollupInfo, PGSecondary, PGSecondaryUniverse, RollupManager}
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

/**
 * Handles creation or updates to rollup tables.  Note that this handles only the metadata, it doesn't
 * actually rebuild the rollups.  That is done separately at a higher level because they need rebuilding
 * when (almost) anything changes.
 */
case class RollupCreatedOrUpdatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                         copyInfo: CopyInfo,
                                         secRollupInfo: SecRollupInfo) {
  private val logger = Logger[RollupCreatedOrUpdatedHandler]

  updateRollupRelationships(
    pgu.datasetMapWriter.createOrUpdateRollupSecondary(copyInfo, new RollupName(secRollupInfo.name), secRollupInfo.soql, None).getOrElse(throw new IllegalStateException("Could not create rollup"))
  )

  def updateRollupRelationships(rollupInfo: LocalRollupInfo): Unit ={
    for(tableName<-parseAndCollectTableNames(rollupInfo)){
      pgu.datasetMapReader.datasetInfoByResourceName(new ResourceName(tableName)) match{
        case Some(dataSetInfo)=>
          logger.info(s"dataset '${rollupInfo.copyInfo.datasetInfo.resourceName.getOrElse(rollupInfo.copyInfo.datasetInfo.systemId)}' rollup '${rollupInfo.name}' relates to dataset '${dataSetInfo.resourceName.getOrElse(dataSetInfo.systemId)}'")
        pgu.datasetMapWriter.createRollupRelationship(rollupInfo,pgu.datasetMapReader.latest(dataSetInfo))
        case None=>
          logger.error(s"Could not find a dataset with identifier '${tableName}'")
      }
    }
  }
}
