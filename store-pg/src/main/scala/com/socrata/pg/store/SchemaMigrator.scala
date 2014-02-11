package com.socrata.pg.store

import com.typesafe.config.Config
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.pg.store.Migration.MigrationOperation.MigrationOperation

/**
 * Performs Liquibase migrations on the pg secondary.
 */
object SchemaMigrator {
  def apply(databaseTree: String, operation: MigrationOperation, config: Config) {
    for {
      dataSourceInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dataSourceInfo.dataSource.getConnection)
    } {
      Migration.migrateDb(conn, operation, "com/socrata/pg/store/schema/migrate.xml", config.getString(s"$databaseTree.database"))
    }
  }
}