package com.socrata.pg.store

import com.rojoma.json.v3.ast.JObject
import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.id.{IndexName, RowId}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, ComputationStrategyInfo, CopyInfo, IndexInfo, RollupInfo}
import com.socrata.soql.environment.ColumnName
import com.typesafe.scalalogging.{Logger => SLogger}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

object PGSecondaryLogger {
  private val logger = SLogger[PGSecondaryLogger[_, _]]
}

class PGSecondaryLogger[CT, CV] extends Logger[CT, CV] {
  import PGSecondaryLogger.logger

  def close(): Unit = logger.debug("closed")

  def truncated(): Unit = logger.debug("truncated")

  def columnCreated(info: ColumnInfo[CT]): Unit = logger.debug("column created")

  def columnRemoved(info: ColumnInfo[CT]): Unit = logger.debug("column removed")

  def computationStrategyCreated(info: ColumnInfo[CT], computationStrategyInfo: ComputationStrategyInfo): Unit =
    logger.debug(s"computation strategy created: $info $computationStrategyInfo")

  def computationStrategyRemoved(info: ColumnInfo[CT]): Unit = logger.debug("computation strategy removed: " + info)

  def fieldNameUpdated(info: ColumnInfo[CT]): Unit = logger.debug("field name updated: " + info)

  def rowIdentifierSet(newIdentifier: ColumnInfo[CT]): Unit = logger.debug("rowIdentifierSet: " + newIdentifier)

  def rowIdentifierCleared(oldIdentifier: ColumnInfo[CT]): Unit = logger.debug("rowIdentifierCleared: " + oldIdentifier)

  def systemIdColumnSet(info: ColumnInfo[CT]): Unit = logger.debug("systemIdColumnSet: " + info)

  def versionColumnSet(info: ColumnInfo[CT]): Unit = logger.debug("versionColumnSet: " + info)

  def workingCopyCreated(info: CopyInfo): Unit = logger.debug("workingCopyCreated: " + info)

  def lastModifiedChanged(lastModified: DateTime): Unit =
    logger.debug("lastModifiedChanged: " + ISODateTimeFormat.dateTime.print(lastModified))

  def dataCopied(): Unit = logger.debug("dataCopied")

  def workingCopyDropped(): Unit = logger.debug("workingCopyDropped")

  def snapshotDropped(info: CopyInfo): Unit = logger.debug("snapshotDropped")

  def workingCopyPublished(): Unit = logger.debug("workingCopyPublished")

  def endTransaction(): Option[Long] = {
    logger.debug("endTransaction")
    Some(0L)
  }

  def insert(systemID: RowId, row: _root_.com.socrata.datacoordinator.Row[CV]): Unit =
    logger.debug("insert: " + systemID + " row: " + row)

  def update(systemID: RowId,
             oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]],
             newRow: _root_.com.socrata.datacoordinator.Row[CV]): Unit =
    logger.debug("update: " + systemID + " oldRow: " + oldRow + " newRow: " + newRow)

  def delete(systemID: RowId, oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]]): Unit =
    logger.debug("delete: " + systemID + " oldRow: " + oldRow)

  def counterUpdated(nextCounter: Long): Unit = logger.debug("counterUpdated")

  def rollupCreatedOrUpdated(info: RollupInfo): Unit = logger.debug(s"rollupCreatedOrUpdated: $info")

  def rollupDropped(info: RollupInfo): Unit = logger.debug(s"rollupDropped: $info")

  def secondaryReindex(): Unit = logger.debug("secondaryReindex")

  def indexDirectiveCreatedOrUpdated(info: ColumnInfo[CT], directive: JObject): Unit = logger.debug(s"indexDirectiveCreatedOrUpdated: $info $directive")

  def indexDirectiveDropped(info: ColumnInfo[CT]): Unit = logger.debug(s"indexDirectiveDropped: $info")

  def indexCreatedOrUpdated(info: IndexInfo): Unit = logger.debug(s"indexCreatedOrUpdated: $info")

  def indexDropped(name: IndexName): Unit =  logger.debug(s"indexDropped: $name")
}
