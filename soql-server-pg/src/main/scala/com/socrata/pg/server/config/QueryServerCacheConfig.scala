package com.socrata.pg.server.config

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class QueryServerCacheConfig(config: Config, root: String) extends ConfigClass(config, root) {
  val maxCachedResults = getInt("max-cached-results")
  val maxResultSize = getInt("max-result-size")
}
