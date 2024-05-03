package com.socrata.pg.store.events

import com.socrata.datacoordinator.id.RollupName
import com.socrata.datacoordinator.secondary.{RollupInfo => SecRollupInfo}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.pg.analyzer2.metatypes.RollupMetaTypes
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
    for(tableNameOrInternalName <- parseAndCollectTableNames(rollupInfo)) {
      val dsInfo = tableNameOrInternalName match {
        case RollupMetaTypes.TableName.ResourceName(tableName) =>
          pgu.datasetMapReader.datasetInfoByResourceName(tableName)
        case RollupMetaTypes.TableName.InternalName(dsInternalName) =>
          pgu.datasetMapReader.datasetInfoByInternalName(dsInternalName)
      }
      dsInfo match {
        case Some(dataSetInfo) if dataSetInfo.systemId != copyInfo.datasetInfo.systemId =>
          logger.info(s"dataset '${rollupInfo.copyInfo.datasetInfo.resourceName}' rollup '${rollupInfo.name}' relates to dataset '${dataSetInfo.resourceName}'")
          pgu.datasetMapWriter.createRollupRelationship(rollupInfo,pgu.datasetMapReader.latest(dataSetInfo))
        case Some(_) =>
          logger.debug("Not adding a relationship from a dataset to itself")
        case None=>
          logger.error(s"Could not find a dataset with identifier '${tableNameOrInternalName}'")
      }
    }
  }
}
