package com.socrata.pg.store

import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.typesafe.config.{ConfigFactory, Config}

object PGSecondaryUtil {

  val config:Config = ConfigFactory.empty()
  val testInternalName = "test-dataset"
  val localeName = "us"
  val obfuscationKey = "key".getBytes

  def populateDatabase(conn: Connection) {
    val sql = DatabasePopulator.createSchema()
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }

  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      populateDatabase(conn)
      f(conn)
    }
  }

}
