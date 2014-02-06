package com.socrata.pg.store

import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers

class PGSecondaryTestBase  extends FunSuite with MustMatchers with BeforeAndAfterAll {
  override def beforeAll = {
    val rootConfig = ConfigFactory.load()
    PropertyConfigurator.configure(Propertizer("log4j", rootConfig.getConfig("com.socrata.soql-server-pg.log4j")))
  }
}
