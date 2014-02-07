package com.socrata.pg.store

import org.scalatest.{Matchers, BeforeAndAfterAll, FunSuite}
import com.socrata.soql.types.{SoQLValue, SoQLType}
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.StandardObfuscationKeyGenerator

class PostgresDatasetInternalNameMapWriterTest extends FunSuite with Matchers with BeforeAndAfterAll {

  type CT = SoQLType
  type CV = SoQLValue

  val common = PostgresUniverseCommon

  override def beforeAll() {
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

  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
      createSchema(conn)
      f(conn)
    }
  }

  test("Writer can insert new rows") {
    withDb() {
      conn => {
        new PostgresDatasetInternalNameMapWriter(conn).create("Dataset Name", new DatasetId(123))
        val datasetId: DatasetId = new PostgresDatasetInternalNameMapReader(conn).datasetIdForInternalName("Dataset Name").get
        datasetId shouldEqual new DatasetId(123)
      }
    }
  }

}
