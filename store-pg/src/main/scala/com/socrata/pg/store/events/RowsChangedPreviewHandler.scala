package com.socrata.pg.store.events

import com.rojoma.json.v3.ast.JString
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, DatasetCopyContext, CopyInfo}
import com.socrata.pg.store.{RollupManager, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

final abstract class RowsChangedPreviewHandler

object RowsChangedPreviewHandler {
  val log = LoggerFactory.getLogger(classOf[RowsChangedPreviewHandler])

  // A change is "sufficiently large" to swap out a copy if it is at least 100k rows and
  // larger than a third of the dataset size.
  def sufficientlyLarge(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                        truthCopyInfo: CopyInfo,
                        truncated: Boolean,
                        totalRowsChanged: Long): Boolean = {
    if(truncated) {
      log.info("Dataset truncated; creating an indexless copy")
      true
    } else if(totalRowsChanged < 100000) {
      false
    } else {
      for {
        stmt <- managed(pgu.conn.prepareStatement("SELECT reltuples :: BIGINT FROM pg_class WHERE relname = ?")).
          and(_.setString(1, truthCopyInfo.dataTableName))
        rs <- managed(stmt.executeQuery())
      } {
        if(!rs.next()) {
          log.warn("Expected a row for reltuples on {} but got nothing", JString(truthCopyInfo.dataTableName))
          false
        } else {
          val count = rs.getLong(1)
          val wantToCopy = totalRowsChanged > 0.3333 * count
          if(wantToCopy) {
            log.info("{} total rows changed; PG estimates existing data is {} rows; creating an indexless copy", totalRowsChanged, count)
          }
          wantToCopy
        }
      }
    }
  }

  def apply(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
            truthDatasetInfo: DatasetInfo,
            truthCopyInfo: CopyInfo,
            truncated: Boolean,
            inserted: Long,
            updated: Long,
            deleted: Long): Option[CopyInfo] = {
    if(sufficientlyLarge(pgu, truthCopyInfo, truncated, inserted + updated + deleted)) {
      val newTruthCopyInfo = pgu.datasetMapWriter.newTableModifier(truthCopyInfo)

      val start = System.nanoTime()

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

      val durationMS = (System.nanoTime() - start) / 1000000
      log.info("Copying the data took {}s", durationMS / 1000.0)

      val rm = new RollupManager(pgu, truthCopyInfo)
      rm.dropRollups(immediate = false)

      pgu.tableDropper.scheduleForDropping(truthCopyInfo.dataTableName)
      pgu.tableDropper.go()
      Some(newTruthCopyInfo)
    } else {
      // ok, we won't be affecting enough rows to justify making a copy
      None
    }
  }
}
