package com.socrata.pg.store

import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.typesafe.config.{ConfigFactory, Config}
import com.socrata.soql.types.{SoQLValue, SoQLType}

object PGSecondaryUtil {
  val config:Config = ConfigFactory.load().getConfig("com.socrata.pg.store")
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

  def withPgu[T]()(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
      f(pgu)
    }
  }

}
