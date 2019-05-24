package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.common.StandardObfuscationKeyGenerator
import scala.language.reflectiveCalls

// scalastyle:off null
class PGSecondaryDatasetMapWriterTest extends PGSecondaryTestBase with PGSecondaryUniverseTestBase with PGStoreTestBase {

  def noopKeyGen(): Array[Byte] = new Array[Byte](0)
  val ZeroID = 0L
  val ZeroVersion = 0L

  override def beforeAll(): Unit = {
    createDatabases()
  }

  override def afterAll(): Unit = {
  }

  def createSchema(conn: Connection): Unit = {
    Migration.migrateDb(conn)
    loadFixtureData(conn)
  }

  def loadFixtureData(conn: Connection): Unit = {
    loadDatasetMapRows(conn)
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

  test("Can create internal name mapping") {
    withDb() {
      conn => {
        createSchema(conn)
        val datasetMapWriter = new PGSecondaryDatasetMapWriter(conn, PostgresUniverseCommon.typeContext.typeNamespace, PostgresUniverseCommon.timingReport, noopKeyGen, ZeroID, ZeroVersion)
        datasetMapWriter.createInternalNameMapping("Dataset Name", new DatasetId(123))
        val datasetId: DatasetId = new PGSecondaryDatasetMapReader(conn).datasetIdForInternalName("Dataset Name").get
        datasetId shouldEqual new DatasetId(123)
      }
    }
  }

  test("Can create dataset only") {
    withPgu() { pgu =>
      createSchema(pgu.conn)
      val datasetId = pgu.secondaryDatasetMapWriter.createDatasetOnly("locale")

      val dataSetInfo = pgu.datasetMapReader.datasetInfo(datasetId).get
      pgu.datasetMapReader.allCopies(dataSetInfo) shouldBe empty
    }
  }

  test("Can delete a copy") {
    withPgu() { pgu =>
      val f = columnsCreatedFixture

      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, None, f.events.iterator)

      val truthCopyInfo = getTruthCopyInfo(pgu, f.datasetInfo)

      val tableName = truthCopyInfo.dataTableName

      pgu.conn.getMetaData().getTables(null, null, tableName, null).next() should be (true)

      pgu.secondaryDatasetMapWriter.deleteCopy(truthCopyInfo)

      an [Exception] should be thrownBy getTruthCopyInfo(pgu, f.datasetInfo) // scalastyle:ignore

      pgu.conn.getMetaData().getTables(null, null, tableName, null).next() should be (false)
    }
  }
}
