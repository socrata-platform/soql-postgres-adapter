package com.socrata.pg.store

import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer

object BuildSchema extends App {
  val config = ConfigFactory.load().getConfig("com.socrata.pg.store")
  PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))

  DatabaseCreator(config, "database")
  DatabaseCreator(config, "test-database")
}
