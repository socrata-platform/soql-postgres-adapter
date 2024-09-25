package com.socrata.pg.server.config

import java.net.URI

import com.typesafe.config.Config
import com.rojoma.json.v3.ast.JString
import com.rojoma.simplearm.v2._
import redis.clients.jedis.JedisPool

import com.socrata.thirdparty.typesafeconfig.ConfigClass

import com.socrata.pg.server.analyzer2.{ResultCache, InMemoryResultCache, JedisResultCache, NoopResultCache}

sealed trait QueryServerCacheConfig {
  def makeCache: Managed[ResultCache]
}

object QueryServerCacheConfig {
  class InMemory(config: Config, root: String) extends ConfigClass(config, root) with QueryServerCacheConfig {
    val maxCachedResults = getInt("max-cached-results")
    val maxResultSize = getInt("max-result-size")

    val makeCache = unmanaged(new InMemoryResultCache(maxCachedResults, maxResultSize))
  }

  class Redis(config: Config, root: String) extends ConfigClass(config, root) with QueryServerCacheConfig {
    val url = new URI(getString("url"))
    val maxResultSize = getInt("max-result-size")

    def makeCache = managed(new JedisPool(url)).map(new JedisResultCache(_, maxResultSize))
  }

  object Noop extends QueryServerCacheConfig {
    val makeCache = unmanaged(NoopResultCache)
  }

  def apply(config: Config, root: String): QueryServerCacheConfig = {
    object Selector extends ConfigClass(config, root) {
      val typ = getString("type")
    }
    Selector.typ match {
      case "memory" => new InMemory(config, root)
      case "redis" => new Redis(config, root)
      case "none" => Noop
      case other => throw new Exception("Unknown cache type " + JString(other))
    }
  }
}
