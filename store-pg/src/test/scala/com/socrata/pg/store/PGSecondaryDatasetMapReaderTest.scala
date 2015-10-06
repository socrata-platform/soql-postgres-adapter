package com.socrata.pg.store

import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.StandardObfuscationKeyGenerator

class PGSecondaryDatasetMapReaderTest extends FunSuite with Matchers with BeforeAndAfterAll
      with PGSecondaryUniverseTestBase with DatabaseTestBase with PGStoreTestBase {

  override def beforeAll(): Unit = {
    createDatabases()
  }

  override def afterAll(): Unit = {}

  def createSchema(conn: Connection): Unit = {
    Migration.migrateDb(conn)
    loadFixtureData(conn)
  }

  def loadFixtureData(conn: Connection): Unit = {
    loadDatasetMapRows(conn)
    loadDatasetInternalNameMapRows(conn)
  }

  def loadDatasetMapRows(conn: Connection): Unit = {
    val sql = "INSERT INTO dataset_map (system_id, next_counter_value, locale_name, obfuscation_key) values (?, ?, ?, ?)"
    using(conn.prepareStatement(sql)) { statement =>
      statement.setLong(1, 123)
      statement.setLong(2, 456)
      statement.setString(3, "us")
      statement.setBytes(4, StandardObfuscationKeyGenerator())
      statement.execute()
    }
  }

  def loadDatasetInternalNameMapRows(conn: Connection): Unit = {
    val sql = "INSERT INTO dataset_internal_name_map (dataset_internal_name, dataset_system_id) values (?, ?)"
    using(conn.prepareStatement(sql)) { stmt =>
      stmt.setString(1, "Dataset Name")
      stmt.setLong(2, 123)
      stmt.execute()
    }
  }

  test("Reader can determine the DatasetId from a given Dataset Internal Name") {
    withDb() { conn => {
      createSchema(conn)
      val datasetId: DatasetId = new PGSecondaryDatasetMapReader(conn).datasetIdForInternalName("Dataset Name").get
      datasetId shouldEqual new DatasetId(123)
    }}
  }

  test("Reader does not raise when Dataset Internal Name cannot be found") {
    withDb() { conn => {
      createSchema(conn)
      new PGSecondaryDatasetMapReader(conn).datasetIdForInternalName("I do not exist")
      // TODO Show Randy how to deal with the "None" case return value from the above method
    }}
  }
}
