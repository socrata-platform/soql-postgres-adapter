package com.socrata.pg.store

import com.typesafe.config.Config
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.rojoma.simplearm.util._

object DatabaseCreator {
  def apply(config: Config, databaseTree: String) {
    for {
      dsInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      DatabasePopulator.populate(conn)
    }
  }
}
