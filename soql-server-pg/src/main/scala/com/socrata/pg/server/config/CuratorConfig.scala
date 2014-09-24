package com.socrata.pg.server.config

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class CuratorConfig(config: Config, root: String) extends ConfigClass(config, root) {

  val ensemble = getStringList("ensemble").mkString(",")
  val namespace = getString("namespace")
  val sessionTimeout = getDuration("session-timeout")
  val connectTimeout = getDuration("connect-timeout")
  val baseRetryWait = getDuration("base-retry-wait")
  val maxRetryWait = getDuration("max-retry-wait")

  val maxRetries = getInt("max-retries")
}
