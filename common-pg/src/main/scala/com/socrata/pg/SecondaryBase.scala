package com.socrata.pg

import java.sql.Connection
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import com.socrata.pg.store.{PostgresUniverseCommon, PGSecondaryUniverse}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import org.slf4j.MDC

trait SecondaryBase {

  val dsConfig: DataSourceConfig

  val postgresUniverseCommon: PostgresUniverseCommon

  protected def populateDatabase(conn: Connection): Unit = { }

  protected def withDb[T](dsInfo:DSInfo)(f: (Connection) => T): T = {
    for {
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      conn.setAutoCommit(false)
      conn.setClientInfo("ApplicationName", applicationNameString)
      f(conn)
      /* We are deliberately not clearing the application name afterwards to avoid the slight overhead of
       * another call and because once in a while it can be useful to see on idle connections what the previous
       * query was about, mirroring how postgres doesn't blank out the "query" field of pg_stat_activity after
       * the query is finished.  All of the uses of this connection pool should go through this acquisition codepath
       * which should avoid any cases of seeing a stale value on an active connection.
       */
    }
  }

  private def applicationNameString: String = {
    // Keep in mind that postgres limits the application_name to 64 chars (unless you recompile).
    // The X-Socrata values are set in the MDC for logging in the query servers, while the "-id" values
    // are set by the secondary watcher processes, so we use whichever we can find.
    Thread.currentThread().getId() + " " +
      Option(MDC.get("X-Socrata-RequestId")).orElse(Option(MDC.get("job-id"))).getOrElse("-") + " " +
      // X-Socrata-Resource is set to the soda fountain resource name, not what the request came into core with.
      // It would be nice to expose the 4x4 the request came into core with but that requires more plumbing work.
      Option(MDC.get("X-Socrata-Resource")).orElse(Option(MDC.get("dataset-id"))).getOrElse("-")
  }

  protected def withPgu[T](dsInfo:DSInfo, truthStoreDatasetInfo:Option[DatasetInfo])
                          (f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb(dsInfo) { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, postgresUniverseCommon, truthStoreDatasetInfo)
      f(pgu)
    }
  }

  protected def withPgu[T](truthStoreDatasetInfo:Option[DatasetInfo])
                          (f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    for(dsInfo <- DataSourceFromConfig(dsConfig)) {
      withPgu(dsInfo, truthStoreDatasetInfo) {
        pgu => f(pgu)
      }
    }
  }
}
