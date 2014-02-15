package com.socrata.pg

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}

trait SecondaryBase {

  val dsConfig: DataSourceConfig

  protected def populateDatabase(conn: Connection) { }

  @volatile var populatedDb = false
  protected def withDb[T]()(f: (Connection) => T): T = {
    // TODO: add back loglevel setting, it is handy
    def loglevel = 0; // 2 = debug, 0 = default
    // TODO: this isn't really the right lifecycle here, recreating this for each withDb call, need to rationalize
    for {
      dsInfo <- DataSourceFromConfig(dsConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
    } yield {
      conn.setAutoCommit(false)
      if (!populatedDb) {
        populateDatabase(conn)
        populatedDb = true
      }
      f(conn)
    }
  }

  protected def withPgu[T]()(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
      f(pgu)
    }
  }
}
