package com.socrata.pg.store

import com.socrata.datacoordinator.truth.loader.Logger
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import scala.Some

class PGSecondaryLogger[CT, CV] extends Logger[CT, CV] {

  def close() { println("closed") }

  def truncated() = println("truncated")

  def columnCreated(info: ColumnInfo[CT]) = println("column created")

  def columnRemoved(info: ColumnInfo[CT]) = println("column removed")

  def rowIdentifierSet(newIdentifier: ColumnInfo[CT]) = println("rowIdentifierSet: " + newIdentifier)

  def rowIdentifierCleared(oldIdentifier: ColumnInfo[CT]) = println("rowIdentifierCleared: " + oldIdentifier)

  def systemIdColumnSet(info: ColumnInfo[CT]) = { println("systemIdColumnSet: " + info)}

  def versionColumnSet(info: ColumnInfo[CT]) = { println("versionColumnSet: " + info)}

  def workingCopyCreated(info: CopyInfo) = { println("workingCopyCreated: " + info)}

  def dataCopied() = println("dataCopied")

  def workingCopyDropped() = println("workingCopyDropped")

  def snapshotDropped(info: CopyInfo) = println("snapshotDropped")

  def workingCopyPublished() = println("workingCopyPublished")

  def endTransaction() = { println("endTransaction"); Some(0L) }

  def insert(systemID: RowId, row: _root_.com.socrata.datacoordinator.Row[CV]) = { println("insert: " + systemID + " row: " + row); }

  def update(systemID: RowId, oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]], newRow: _root_.com.socrata.datacoordinator.Row[CV]) = { println("update: " + systemID + " oldRow: " + oldRow + " newRow: " + newRow); }

  def delete(systemID: RowId, oldRow: Option[_root_.com.socrata.datacoordinator.Row[CV]]) = { println("delete: " + systemID + " oldRow: " + oldRow); }

  def counterUpdated(nextCounter: Long) = { println("counterUpdated")}
}
