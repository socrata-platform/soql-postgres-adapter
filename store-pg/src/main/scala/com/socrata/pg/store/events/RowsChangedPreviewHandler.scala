package com.socrata.pg.store.events

import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetCopyContext, CopyInfo}
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}

object RowsChangedPreviewHandler {
  def sufficientlyLarge(totalRowsChanged: Long): Boolean = totalRowsChanged > 20

  def apply(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
            truthDatasetInfo: DatasetInfo,
            truthCopyInfo: CopyInfo,
            truncated: Boolean,
            inserted: Long,
            updated: Long,
            deleted: Long): Option[CopyInfo] = {
    if(truncated || sufficientlyLarge(inserted + updated + deleted)) {
      val newTruthCopyInfo = pgu.datasetMapWriter.newTableModifier(truthCopyInfo)

      val logger = pgu.logger(truthDatasetInfo, "_no_user")
      val schemaLoader = pgu.schemaLoader(logger)
      val schema = pgu.datasetMapReader.schema(newTruthCopyInfo)
      schemaLoader.create(newTruthCopyInfo)
      schemaLoader.addColumns(schema.values)

      if(!truncated) {
        val copier = pgu.datasetContentsCopier(logger)
        val schema = pgu.datasetMapReader.schema(newTruthCopyInfo)
        val copyCtx = new DatasetCopyContext[SoQLType](newTruthCopyInfo, schema)
        copier.copy(truthCopyInfo, copyCtx)
      }

      schema.values.find(_.isSystemPrimaryKey).foreach(schemaLoader.makeSystemPrimaryKey)

      pgu.tableDropper.scheduleForDropping(truthCopyInfo.dataTableName)
      pgu.tableDropper.go()
      Some(newTruthCopyInfo)
    } else {
      // ok, we won't be affecting enough rows to justify making a copy
      None
    }
  }
}
