package com.socrata.pg.analyzer2.metatypes

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap

import com.socrata.datacoordinator.id.{UserColumnId, DatasetInternalName}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.pg.query.QueryServerHelper

trait CopyCache[MT <: MetaTypes] extends LabelUniverse[MT] {
  def apply(dtn: DatabaseTableName): Option[(CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])]
  def asMap: Map[DatabaseTableName, (CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])]

  def allCopies: Seq[CopyInfo]
}
