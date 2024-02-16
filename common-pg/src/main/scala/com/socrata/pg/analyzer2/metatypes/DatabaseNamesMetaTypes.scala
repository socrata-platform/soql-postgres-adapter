package com.socrata.pg.analyzer2.metatypes

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.prettyprint.prelude._

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.SoQLValue
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.SoQLTypeInfo2

import com.socrata.pg.analyzer2.SoQLValueDebugHelper

sealed abstract class AugmentedTableName {
  val name: String // This is the actual name of the physical database table
}
object AugmentedTableName {
  case class TTable(name: String) extends AugmentedTableName
  case class RollupTable(name: String) extends AugmentedTableName
}

final abstract class DatabaseNamesMetaTypes extends MetaTypes with SoQLMetaTypesExt {
  override type ResourceNameScope = DatabaseMetaTypes#ResourceNameScope
  override type ColumnType = DatabaseMetaTypes#ColumnType
  override type ColumnValue = DatabaseMetaTypes#ColumnValue
  override type DatabaseTableNameImpl = AugmentedTableName
  override type DatabaseColumnNameImpl = String
}

object DatabaseNamesMetaTypes extends MetaTypeHelper[DatabaseNamesMetaTypes] {
  val typeInfo = new SoQLTypeInfo2[DatabaseNamesMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[DatabaseNamesMetaTypes] {
    // These are database table names, so we can mark rollups by a
    // thing that would not be valid database table name syntax.
    private val rollupTag = "rollup:"

    def fromProvenance(prov: Provenance): types.DatabaseTableName[DatabaseNamesMetaTypes] = {
      val rawProv = prov.value
      if(rawProv.startsWith(rollupTag)) {
        DatabaseTableName(AugmentedTableName.RollupTable(rawProv.substring(rollupTag.length)))
      } else {
        DatabaseTableName(AugmentedTableName.TTable(rawProv))
      }
    }

    def toProvenance(dtn: types.DatabaseTableName[DatabaseNamesMetaTypes]): Provenance = {
      dtn match {
        case DatabaseTableName(AugmentedTableName.TTable(name)) => Provenance(name)
        case DatabaseTableName(AugmentedTableName.RollupTable(name)) => Provenance(rollupTag + name)
      }
    }
  }

  def rewriteDTN(dtn: types.DatabaseTableName[DatabaseMetaTypes]): types.DatabaseTableName[DatabaseNamesMetaTypes] = {
    val DatabaseTableName(copyInfo) = dtn
    DatabaseTableName(AugmentedTableName.TTable(copyInfo.dataTableName))
  }

  def rewriteFrom(dmtState: DatabaseMetaTypes, analysis: SoQLAnalysis[DatabaseMetaTypes]): SoQLAnalysis[DatabaseNamesMetaTypes] = {
    analysis.rewriteDatabaseNames[DatabaseNamesMetaTypes](
      rewriteDTN,
      { case (dtn, DatabaseColumnName(columnInfo)) => DatabaseColumnName(columnInfo.physicalColumnBase) },
      dmtState.provenanceMapper,
      provenanceMapper,
      typeInfo.updateProvenance
    )
  }

  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
    implicit object atnHasDoc extends HasDoc[AugmentedTableName] {
      def docOf(v: AugmentedTableName) = Doc(v.name)
    }
  }
}
