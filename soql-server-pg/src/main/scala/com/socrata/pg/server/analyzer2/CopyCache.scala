package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._

import com.socrata.datacoordinator.id.{UserColumnId, DatasetInternalName}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.pg.query.QueryServerHelper

class CopyCache(pgu: PGSecondaryUniverse[InputMetaTypes#ColumnType, InputMetaTypes#ColumnValue]) extends LabelUniverse[InputMetaTypes] {
  private val map = new scm.HashMap[DatabaseTableName, (CopyInfo, Map[UserColumnId, ColumnInfo[CT]])]

  // returns None if the database table name is unknown to this secondary
  def apply(dtn: DatabaseTableName): Option[(CopyInfo, Map[UserColumnId, ColumnInfo[CT]])] =
    map.get(dtn).orElse {
      val DatabaseTableName((internalName, Stage(lifecycleStage))) = dtn
      QueryServerHelper.getCopy(pgu, internalName.underlying, Some(lifecycleStage)).map { copy =>
        val schemaBySystemId = pgu.datasetMapReader.schema(copy)
        val schemaByUserId = schemaBySystemId.values.map { colInfo => colInfo.userColumnId -> colInfo }.toMap
        map += dtn -> ((copy, schemaByUserId))
        (copy, schemaByUserId)
      }
    }

  def mostRecentlyModifiedAt = {
    val it = map.valuesIterator.map(_._1.lastModified)
    if(it.hasNext) Some(it.maxBy(_.getMillis))
    else None
  }

  // All the dataVersions of all the datasets involved, in some
  // arbitrary but consistent order.
  def orderedVersions =
    map.to.toSeq.sortBy { case (DatabaseTableName((DatasetInternalName(instance, dsId), Stage(stage))), (copyInfo, _)) =>
      (instance, dsId.underlying, stage, copyInfo.copyNumber)
    }.map { case (_dtn, (copyInfo, _colInfos)) => copyInfo.dataVersion }
}
