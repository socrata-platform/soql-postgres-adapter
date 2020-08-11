package com.socrata.pg.store

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.pg.Schema._
import com.socrata.pg.query.PGQueryTestBase
import com.socrata.pg.server.{PGQueryServerDatabaseTestBase, QueryServerTest}
import com.socrata.pg.store.PGSecondaryUtil._

import scala.io.Source
import scala.language.reflectiveCalls

class SchemaTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {
  override def beforeAll: Unit = createDatabases()

  test("schema json codec") {
    val dsConfig = new DataSourceConfig(config, "database")
    val ds = DataSourceFromConfig(dsConfig)
    ds.map { dsInfo =>
      withPgu() { pgu =>
      val f = columnsCreatedFixture
      f.pgs.doVersion(pgu, f.datasetInfo, f.dataVersion + 1, f.dataVersion + 1, None, f.events.iterator)
      val qs = new QueryServerTest(dsInfo, pgu)
      val schema = qs.getSchema(f.datasetInfo.internalName, None).get
      val schemaj = JsonUtil.renderJson(schema)
      val schemaRoundTrip = JsonUtil.parseJson[com.socrata.datacoordinator.truth.metadata.Schema](schemaj)
        .right.toOption.get
      schema should be (schemaRoundTrip)
      val expected = Source.fromURL(getClass.getResource("/fixtures/schema.json"))
      val expectedSchema = JsonUtil.readJson[com.socrata.datacoordinator.truth.metadata.Schema](expected.reader())
        .right.toOption.get
      schema should be (expectedSchema)
      }
    }
  }
}
