package com.socrata.pg.config

import com.socrata.curator.CuratorConfig
import com.typesafe.config.{ConfigUtil, Config}
import com.socrata.thirdparty.typesafeconfig.ConfigClass
import com.socrata.datacoordinator.common.DataSourceConfig
import scala.collection.JavaConverters._

class StoreConfig(config: Config, root: String) extends ConfigClass(config, root) {
  private val defaultResyncBatchSize = 1000

  // handle blank root
  override protected def path(key: String*): String = {
    val fullKey = if (root.isEmpty) key else ConfigUtil.splitPath(root).asScala ++ key
    ConfigUtil.joinPath(fullKey: _*)
  }

  val database = new DataSourceConfig(config, path("database"))

  val tablespace = optionally(getString("tablespace"))

  val resyncBatchSize = optionally(getInt("resyncBatchSize")).getOrElse(defaultResyncBatchSize)

  val curatorConfig = optionally(getRawConfig("curator")).map { _ =>
    getConfig("curator", new CuratorConfig(_, _))
  }

  val secondaryMetrics = SecondaryMetricsConfig(config, path("secondary-metrics"))

  val transparentCopyFunction = optionally(getString("transparent-copy-function"))
}

case class SecondaryMetricsConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val enabled = optionally(getBoolean("enabled")).getOrElse(false)
}
