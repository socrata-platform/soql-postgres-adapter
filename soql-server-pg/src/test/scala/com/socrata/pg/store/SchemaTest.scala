package com.socrata.pg.store

import com.rojoma.json.util.JsonUtil
import com.socrata.pg.server.{PGQueryServerDatabaseTestBase, QueryServerTest}
import com.socrata.pg.Schema
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import scala.language.reflectiveCalls
import scala.io.Source

class SchemaTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {
  import com.socrata.pg.store.PGSecondaryUtil._
  import Schema._

  override def beforeAll() {
    createDatabases()
  }

  test("schema json codec") {
    val dsConfig = new DataSourceConfig(config, "database")
    val ds = DataSourceFromConfig(dsConfig)
    for (dsInfo <- ds) {
      withPgu() { pgu =>
      val f = columnsCreatedFixture
      f.pgs._version(pgu, f.datasetInfo, f.dataVersion+1, None, f.events.iterator)
      val qs = new QueryServerTest(dsInfo, pgu)
      val schema = qs.getSchema(testInternalName, None).get
      val schemaj = JsonUtil.renderJson(schema)
      val schemaRoundTrip = JsonUtil.parseJson[com.socrata.datacoordinator.truth.metadata.Schema](schemaj).get
      schema should be (schemaRoundTrip)
      val expected = Source.fromURL(getClass.getResource("/fixtures/schema.json"))
      val expectedSchema = JsonUtil.readJson[com.socrata.datacoordinator.truth.metadata.Schema](expected.reader()).get
      schema should be (expectedSchema)
      }
    }
  }
}
