package com.socrata.pg.analyzer2.rollup

import scala.collection.{mutable => scm}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName

import com.socrata.pg.analyzer2.{RollupRewriter, SqlizerUniverse}

trait RollupInfo[MT <: MetaTypes] extends SqlizerUniverse[MT] {
  val statement: Statement
  val resourceName: types.ScopedResourceName[MT]
  val databaseName: DatabaseTableName

  def databaseColumnNameOfIndex(idx: Int): DatabaseColumnName

  lazy val description = locally {
    val columnMap = statement.schema.iterator.zipWithIndex.map { case ((label, _), idx) =>
      label -> databaseColumnNameOfIndex(idx)
    }.toMap

    TableDescription.Dataset[MT](
      databaseName,
      RollupRewriter.MAGIC_ROLLUP_CANONICAL_NAME,
      OrderedMap() ++ statement.schema.iterator.map { case (label, schemaEnt) =>
        columnMap(label) -> TableDescription.DatasetColumnInfo(
          schemaEnt.name,
          schemaEnt.typ,
          false
        )
      },
      Nil,
      statement.unique.map { columnLabels =>
        columnLabels.map(columnMap)
      }.toVector
    )
  }

  def from(labelProvider: LabelProvider) =
    FromTable(
      description.name,
      description.canonicalName,
      resourceName,
      None,
      labelProvider.tableLabel(),
      OrderedMap() ++ description.columns.iterator.map { case (dtn, dci) =>
        dtn -> NameEntry(dci.name, dci.typ)
      },
      description.primaryKeys
    )
}
