package com.socrata.pg.store

import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import scala.Some
import com.typesafe.scalalogging.slf4j.Logging

class PGSecondaryLogger[CT, CV] extends Logger[CT, CV] with Logging {

  def close() { logger.debug("closed") }

  def truncated() = logger.debug("truncated")

  def columnCreated(info: ColumnInfo[CT]) = logger.debug("column created")

  def columnRemoved(info: ColumnInfo[CT]) = logger.debug("column removed")

  def rowIdentifierSet(newIdentifier: ColumnInfo[CT]) = logger.debug("rowIdentifierSet: " + newIdentifier)

  def rowIdentifierCleared(oldIdentifier: ColumnInfo[CT]) = logger.debug("rowIdentifierCleared: " + oldIdentifier)

  def systemIdColumnSet(info: ColumnInfo[CT]) = { logger.debug("systemIdColumnSet: " + info)}

  def versionColumnSet(info: ColumnInfo[CT]) = { logger.debug("versionColumnSet: " + info)}

  def workingCopyCreated(info: CopyInfo) = { logger.debug("workingCopyCreated: " + info)}

  def dataCopied() = logger.debug("dataCopied")

  def workingCopyDropped() = logger.debug("workingCopyDropped")

  def snapshotDropped(info: CopyInfo) = logger.debug("snapshotDropped")

  def workingCopyPublished() = logger.debug("workingCopyPublished")

  def endTransaction() = { logger.debug("endTransaction"); Some(0L) }

  def insert(systemID: RowId, row: _root_.com.socrata.datacoordinator.Row[CV]) = { logger.debug("insert: " + systemID + " row: " + row); }

  def update(systemID: RowId, oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]], newRow: _root_.com.socrata.datacoordinator.Row[CV]) = { logger.debug("update: " + systemID + " oldRow: " + oldRow + " newRow: " + newRow); }

  def delete(systemID: RowId, oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]]) = { logger.debug("delete: " + systemID + " oldRow: " + oldRow); }

  def counterUpdated(nextCounter: Long) = { logger.debug("counterUpdated")}
}
