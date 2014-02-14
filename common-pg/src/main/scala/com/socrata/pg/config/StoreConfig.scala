package com.socrata.pg.config

import com.typesafe.config.Config

class StoreConfig(config: Config, val root: String) extends ConfigHelper {

  class C3P0(val root: String) extends ConfigHelper {
    val maxPoolSize = config.getInt(k("maxPoolSize"))
    val idleConnectionTestPeriod = config.getInt(k("idleConnectionTestPeriod"))
    val testConnectionOnCheckin = config.getBoolean(k("testConnectionOnCheckin"))
    val preferredTestQuery = config.getString(k("preferredTestQuery"))
    val maxIdleTimeExcessConnections = config.getInt(k("maxIdleTimeExcessConnections"))
  }

  class DatabaseConfig(val root: String) extends ConfigHelper {
    val host = config.getString(k("host"))
    val port = config.getInt(k("port"))
    val username = config.getString(k("username"))
    val password = config.getString(k("password"))
    val c3p0 = new C3P0(k("c3p0"))
  }

  val database = new DatabaseConfig(k("database"))
}