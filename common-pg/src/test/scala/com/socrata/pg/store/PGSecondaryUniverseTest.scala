package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.soql.types.{SoQLValue, SoQLType}


/**
 *
 */
class PGSecondaryUniverseTest extends FunSuite with MustMatchers with PropertyChecks with BeforeAndAfterAll {
    type CT = SoQLType
    type CV = SoQLValue
    val common = new SoQLCommon()
    override def beforeAll() {
    }

    override def afterAll() {
    }

    def withDB[T]()(f: Connection => T): T = {
      using(DriverManager.getConnection("jdbc:h2:mem:")) { conn =>
        conn.setAutoCommit(false)
        f(conn)
      }
    }

    test("Universe can create a table") {
      withDB() { conn =>
        val pgu = new PGSecondaryUniverse[CT, CV](conn,  common )
      }
    }


}
