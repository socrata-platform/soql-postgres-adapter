package com.socrata.pg.store.events

import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.pg.store._
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.{CopyInfo => TruthCopyInfo}

case class SystemRowIdentifierChangedHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], truthCopyInfo: TruthCopyInfo, secColumnInfo: ColumnInfo[SoQLType]) {
  val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])

  val schema = pgu.datasetMapReader.schema(truthCopyInfo)

  schema.values.filter(_.isSystemPrimaryKey).foreach { col =>
    throw new UnsupportedOperationException(s"Can't modify existing system primary key of ${col}")
  }

  val truthColumnInfo = schema(secColumnInfo.systemId)

  val newTruthColumnInfo = pgu.datasetMapWriter.setSystemPrimaryKey(truthColumnInfo)

  sLoader.makeSystemPrimaryKey(newTruthColumnInfo)
}
