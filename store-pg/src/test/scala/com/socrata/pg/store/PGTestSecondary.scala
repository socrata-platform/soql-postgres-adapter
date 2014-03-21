package com.socrata.pg.store

import com.typesafe.config.Config
import com.socrata.datacoordinator.common.DataSourceConfig

class PGTestSecondary(config: Config, val database: String) extends PGSecondary(config) {

  override val dsConfig = new DataSourceConfig(config, database)

}
