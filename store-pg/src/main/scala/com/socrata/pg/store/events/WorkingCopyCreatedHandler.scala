package com.socrata.pg.store.events

import com.socrata.pg.store.{PGSecondaryLogger, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.secondary.{CopyInfo => SecondaryCopyInfo, DatasetInfo => SecondaryDatasetInfo}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo, LifecycleStage}
import com.rojoma.simplearm.util._

/**
 * Handles WorkingCopyCreated Event
 */
case class WorkingCopyCreatedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                     datasetId: Option[com.socrata.datacoordinator.id.DatasetId],
                                     datasetInfo:SecondaryDatasetInfo,
                                     copyInfo:SecondaryCopyInfo) {

  copyInfo.copyNumber match {
    case 1 =>
      val (copyInfoSecondary:TruthCopyInfo, sLoader) = createDataset(pgu, datasetInfo.localeName)
      pgu.secondaryDatasetMapWriter.createInternalNameMapping(datasetInfo.internalName, copyInfoSecondary.datasetInfo.systemId)
    case copyNumBeyondOne =>
      pgu.datasetMapReader.datasetInfo(datasetId.get).map { truthDatasetInfo =>
        val copyIdFromSequence = using(pgu.conn.prepareStatement("select nextval('copy_map_system_id_seq')")) { stmt =>
          val rs = stmt.executeQuery()
          if (rs.next()) new com.socrata.datacoordinator.id.CopyId(rs.getLong(1))
          else throw new Exception("cannot get new copy id from sequence")
        }
        val newCopyInfo = pgu.datasetMapWriter.unsafeCreateCopy(
          truthDatasetInfo,
          copyIdFromSequence,
          copyNumBeyondOne,
          com.socrata.datacoordinator.truth.metadata.LifecycleStage.valueOf(copyInfo.lifecycleStage.toString),
          copyInfo.dataVersion)
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        sLoader.create(newCopyInfo)
      }
  }

  private def createDataset(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], locale:String):(TruthCopyInfo, SchemaLoader[SoQLType]) = {
    val copyInfo = pgu.datasetMapWriter.create(locale)
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    // TODO figure out best way to avoid creating unused log and audit tables.
    sLoader.create(copyInfo)
    (copyInfo, sLoader)
  }
}
