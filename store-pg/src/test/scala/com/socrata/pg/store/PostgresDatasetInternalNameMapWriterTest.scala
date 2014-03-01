package com.socrata.pg.store

import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.StandardObfuscationKeyGenerator

class PostgresDatasetInternalNameMapWriterTest extends FunSuite with Matchers with BeforeAndAfterAll
      with PGSecondaryUniverseTestBase with DatabaseTestBase with PGStoreTestBase {

  override def beforeAll() {
    createDatabases()
  }

  override def afterAll() {
  }

  def createSchema(conn: Connection) {
    val sql = DatabasePopulator.createSchema()
    using(conn.createStatement()) { stmt => stmt.execute(sql) }
    loadFixtureData(conn)
  }

  def loadFixtureData(conn: Connection) {
    loadDatasetMapRows(conn)
  }

  def loadDatasetMapRows(conn: Connection) {
    val sql = "INSERT INTO dataset_map (system_id, next_counter_value, locale_name, obfuscation_key) values (?, ?, ?, ?)"
    using(conn.prepareStatement(sql)) { statement =>
      statement.setLong(1, 123)
      statement.setLong(2, 456)
      statement.setString(3, "us")
      statement.setBytes(4, StandardObfuscationKeyGenerator())
      statement.execute()
    }
  }

  test("Writer can insert new rows") {
    withDb() {
      conn => {
        createSchema(conn)
        new PostgresDatasetInternalNameMapWriter(conn).create("Dataset Name", new DatasetId(123))
        val datasetId: DatasetId = new PostgresDatasetInternalNameMapReader(conn).datasetIdForInternalName("Dataset Name").get
        datasetId shouldEqual new DatasetId(123)
      }
    }
  }
}
