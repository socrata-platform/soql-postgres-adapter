package com.socrata.pg.store.ddl

import java.sql.Connection
import com.socrata.pg.store.{PGSecondaryLogger, PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.loader.SchemaLoader

/**
 * Dataset Schema Transformation
 */
object DatasetSchema {

  def createTable(conn:Connection, locale:String):(PGSecondaryUniverse[SoQLType, SoQLValue], CopyInfo, SchemaLoader[SoQLType]) = {
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
    val copyInfo = pgu.datasetMapWriter.create(locale)
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(copyInfo)
    (pgu, copyInfo, sLoader)
  }

}
