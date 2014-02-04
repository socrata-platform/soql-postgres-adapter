package com.socrata.pg.store

import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.rojoma.simplearm.util._

object DatabaseCreator {
  def apply(databaseTree: String) {
    val config = ConfigFactory.load
    println(config.root.render)

    for {
      dsInfo <- DataSourceFromConfig(new DataSourceConfig(config, databaseTree))
      conn <- managed(dsInfo.dataSource.getConnection)
    } {
      DatabasePopulator.populate(conn)
    }
  }
}
