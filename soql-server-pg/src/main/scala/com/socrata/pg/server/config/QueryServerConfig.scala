package com.socrata.pg.server.config

import com.typesafe.config.Config
import com.socrata.pg.config.{StoreConfig}
import com.socrata.thirdparty.curator.CuratorConfig
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class QueryServerConfig(val config: Config, val root: String) extends ConfigClass(config, root) {
  val log4j = getRawConfig("log4j")
  val store = new StoreConfig(config, path("store"))
  val port = getInt("port")
  val curator = new CuratorConfig(config, path("curator"))
  val advertisement = new AdvertisementConfig(config, path("service-advertisement"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))
  val instance = getString("instance")
}
