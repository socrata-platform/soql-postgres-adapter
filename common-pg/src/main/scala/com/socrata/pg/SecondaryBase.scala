package com.socrata.pg

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo

trait SecondaryBase {

  val dsConfig: DataSourceConfig

  val postgresUniverseCommon: PostgresUniverseCommon

  protected def populateDatabase(conn: Connection) { }

  protected def withDb[T](dsInfo:DSInfo)(f: (Connection) => T): T = {
    for {
      conn <- managed(dsInfo.dataSource.getConnection)
    } yield {
      conn.setAutoCommit(false)
      f(conn)
    }
  }

  protected def withPgu[T](dsInfo:DSInfo, truthStoreDatasetInfo:Option[DatasetInfo])(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb(dsInfo) { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, postgresUniverseCommon, truthStoreDatasetInfo)
      f(pgu)
    }
  }

  protected def withPgu[T](truthStoreDatasetInfo:Option[DatasetInfo])(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    for {
      dsInfo <- DataSourceFromConfig(dsConfig)
    } yield {
      withPgu(dsInfo, truthStoreDatasetInfo) {
        pgu => f(pgu)
      }
    }
  }
}
