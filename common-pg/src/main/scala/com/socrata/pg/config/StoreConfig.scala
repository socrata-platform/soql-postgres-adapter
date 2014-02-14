package com.socrata.pg.config

import com.typesafe.config.Config
import com.socrata.thirdparty.typesafeconfig.ConfigClass

class StoreConfig(config: Config, root: String) extends ConfigClass(config, root) {

  class C3P0(val root: String) extends ConfigClass(config, root) {
    val maxPoolSize = getInt("maxPoolSize")
    val idleConnectionTestPeriod = getInt("idleConnectionTestPeriod")
    val testConnectionOnCheckin = config.getBoolean(path("testConnectionOnCheckin"))
    val preferredTestQuery = getString("preferredTestQuery")
    val maxIdleTimeExcessConnections = getInt("maxIdleTimeExcessConnections")
  }

  class DatabaseConfig(val root: String) extends ConfigClass(config, root) {
    val host = getString("host")
    val port = getInt("port")
    val username = getString("username")
    val password = getString("password")
    val c3p0 = new C3P0(path("c3p0"))
  }

  val database = new DatabaseConfig(path("database"))
}