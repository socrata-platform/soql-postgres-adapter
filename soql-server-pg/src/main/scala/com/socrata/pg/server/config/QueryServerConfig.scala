package com.socrata.pg.server.config

import com.typesafe.config.Config
import com.socrata.pg.config.{StoreConfig, ConfigHelper}

class QueryServerConfig(val config: Config, val root: String) extends ConfigHelper {
  val log4j = config.getConfig(k("log4j"))
  val store = new StoreConfig(config, k("store"))
  val port = config.getInt(k("port"))
  val curator = new CuratorConfig(config, k("curator"))
  val advertisement = new AdvertisementConfig(config, k("service-advertisement"))
  val instance = config.getString(k("instance"))
}
