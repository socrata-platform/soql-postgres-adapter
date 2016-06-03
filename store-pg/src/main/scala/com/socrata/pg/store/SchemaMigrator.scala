package com.socrata.pg.store

import com.typesafe.config.Config
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation.MigrationOperation

/**
 * Performs Liquibase migrations on the pg secondary.
 */
object SchemaMigrator {
  def apply(databaseTree: String, operation: MigrationOperation, config: Config): Unit = {
    for {
      dataSourceInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dataSourceInfo.dataSource.getConnection)
      stmt <- managed(conn.createStatement())
    } {
      stmt.execute("SET lock_timeout = '30s'")
      Migration.migrateDb(conn, operation)
    }
  }
}
