package com.socrata.pg.server.config

import com.typesafe.config.Config
import com.socrata.http.server.livenesscheck.LivenessCheckConfig
import com.socrata.pg.config.{StoreConfig}
import com.socrata.curator.{CuratorConfig, DiscoveryConfig}
import com.socrata.thirdparty.metrics.MetricsOptions
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.typesafe.scalalogging.Logger

class QueryServerConfig private(val config: Config, val root: String) extends ConfigClass(config, root) {
  val log4j = getRawConfig("log4j")
  val store = new StoreConfig(config, path("store"))
  val port = getInt("port")
  val curator = new CuratorConfig(config, path("curator"))
  val discovery = new DiscoveryConfig(config, path("service-advertisement"))
  val livenessCheck = new LivenessCheckConfig(config, path("liveness-check"))
  val metrics = MetricsOptions(config.getConfig(path("metrics")))
  val instance = getString("instance")
  val threadpool = getRawConfig("threadpool")
  val maxConcurrentRequestsPerDataset = getInt("max-concurrent-requests-per-dataset")
  val leadingSearch = getBoolean("leading-search")
  val httpQueryTimeoutDelta = getDuration("http-query-timeout-delta")
}

object QueryServerConfig {
  private val removePasswords: String => String = _.replaceAll("""(\".*password.*\" : \")(.*?)(\")""", "$1***$3")
  private val logger = Logger[QueryServerConfig]
  def apply(config: Config, root: String): QueryServerConfig = {
    val queryServerConfig = new QueryServerConfig(config, root)
    logger.info("Configuration:\n" + removePasswords(queryServerConfig.root.render))
    queryServerConfig
  }
}
