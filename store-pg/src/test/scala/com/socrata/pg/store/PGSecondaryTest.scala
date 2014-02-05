package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.secondary._
import com.typesafe.config.{ConfigFactory, Config}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.soql.types._
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.secondary.ColumnInfo
import com.socrata.datacoordinator.secondary.WorkingCopyCreated
import com.socrata.datacoordinator.secondary.ColumnCreated
import com.socrata.datacoordinator.secondary.CopyInfo
import com.socrata.datacoordinator.secondary.DatasetInfo
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.pg.store.events.WorkingCopyCreatedEventHandler

/**
 * Test... the PGSecondary.
 */
class PGSecondaryTest  extends FunSuite with MustMatchers with BeforeAndAfterAll {

  val config:Config = ConfigFactory.empty()
  val testInternalName = "test-dataset"
  val localeName = "us"
  val obfuscationKey = "key".getBytes

  // TOOD cleanup temporary log hack
  override def beforeAll = {
    val rootConfig = ConfigFactory.load()
    PropertyConfigurator.configure(Propertizer("log4j", rootConfig.getConfig("com.socrata.soql-server-pg.log4j")))
  }

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


  test("can handle working copy event") {
     withDb() { conn =>
        val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
        val dataVersion = 0L
        val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
        WorkingCopyCreatedEventHandler(datasetInfo, dataVersion, copyInfo, conn)
     }
  }

  test("refuse to create working copies with copy ids != 1") {

  }

  test("fail when we create a new dataset with a copy id != 1") {

  }

  test("handle ColumnCreated") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 2L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
          WorkingCopyCreated(copyInfo),
          ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId("mysystemidcolumn"), SoQLText, true, false, false)),
          ColumnCreated(ColumnInfo(new ColumnId(9125), new UserColumnId("myuseridcolumn"), SoQLText, false, true, false)),
          ColumnCreated(ColumnInfo(new ColumnId(9126), new UserColumnId("myversioncolumn"), SoQLText, false, false, true)),
          ColumnCreated(ColumnInfo(new ColumnId(9127), new UserColumnId("mycolumn"), SoQLText, false, false, false)),
          ColumnCreated(ColumnInfo(new ColumnId(9128), new UserColumnId(":updated_at"), SoQLTime, false, false, false))
        ).iterator
      pgs._version(datasetInfo, dataVersion, None, events, conn)
    }
  }

  test("handle SystemRowIdentifierChanged") {
    withDb() { conn =>
      val datasetInfo = DatasetInfo(testInternalName, localeName, obfuscationKey)
      val dataVersion = 2L
      val copyInfo = CopyInfo(new CopyId(-1), 1, LifecycleStage.Published, dataVersion)
      val pgs = new PGSecondary(config)
      val events = Seq(
          WorkingCopyCreated(copyInfo),
          ColumnCreated(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false)),
          SystemRowIdentifierChanged(ColumnInfo(new ColumnId(9124), new UserColumnId(":id"), SoQLID, false, false, false))
        ).iterator
      pgs._version(datasetInfo, dataVersion, None, events, conn)
    }
  }
}
