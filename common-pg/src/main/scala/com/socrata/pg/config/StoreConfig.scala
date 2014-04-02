package com.socrata.pg.config

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.datacoordinator.common.DataSourceConfig

class StoreConfig(config: Config, root: String) extends ConfigClass(config, root) {

  protected override def path(key: String*) = super.path(key: _*).stripPrefix(".") // handle blank root

  val database = new DataSourceConfig(config, path("database"))

  val tablespace = optionally(getString("tablespace"))

}