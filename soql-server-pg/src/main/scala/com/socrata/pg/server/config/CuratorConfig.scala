package com.socrata.pg.server.config

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class CuratorConfig(config: Config, root: String) extends ConfigClass(config, root) {

  val ensemble = config.getStringList(path("ensemble")).asScala.mkString(",")
  val namespace = getString("namespace")
  val sessionTimeout = config.getMilliseconds(path("session-timeout")).longValue.millis
  val connectTimeout = config.getMilliseconds(path("connect-timeout")).longValue.millis
  val baseRetryWait = config.getMilliseconds(path("base-retry-wait")).longValue.millis
  val maxRetryWait = config.getMilliseconds(path("max-retry-wait")).longValue.millis
  val maxRetries = getInt("max-retries")
}
