package com.socrata.pg.server

import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import com.socrata.datacoordinator.id.{RowId, UserColumnId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store._
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.environment.DatasetContext
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.soql.collection.OrderedMap
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.soql.SoQLAnalysis
import java.sql.Connection
import org.apache.log4j.PropertyConfigurator
import scala.language.existentials
import com.socrata.pg.soql.CaseSensitive
import com.socrata.http.server.util.NoPrecondition

class QueryTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase {

  import QueryTest._

  override def beforeAll = {
    PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))
    createDatabases()
  }

  test("select text, number") {
    withDb() { conn =>
      val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
      val schema = createTableWithSchema(pgu, copyInfo, sLoader)

      // Setup our row data
      val dummyVals = dummyValues()
      insertDummyRow(new RowId(0), dummyVals, pgu, copyInfo, schema)

      val result = getRow(new RowId(0), pgu, copyInfo, schema)
      assert(result.size == 1)
      val row = result.get(SoQLID(0)).get
      val rowValues = row.row.values.toSet

      // Check that all our dummy values can be read; except for json.
      dummyVals filterKeys (!Set(TypeName("json")).contains(_)) foreach {
        (v) => assert(rowValues.contains(v._2), "Could not find " + v + " in row values: " + rowValues)
      }

      val idMap =  (cn: ColumnName) => new UserColumnId(cn.name)
      val soql = "select text_USERNAME, number_USERNAME"

      for (readCtx <- pgu.datasetReader.openDataset(copyInfo)) yield {
        val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
        val columnNameTypeMap: OrderedMap[ColumnName, SoQLType] = baseSchema.values.foldLeft(OrderedMap.empty[ColumnName, SoQLType]) { (map, cinfo) =>
          map + (ColumnName(cinfo.userColumnId.underlying) -> cinfo.typ)
        }
        val datasetCtx = new DatasetContext[SoQLType] {
          val schema = columnNameTypeMap
        }
        val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
        val (requestColumns, version, mresult) =
          for (dsInfo <- ds) yield {
            val qs = new QueryServer(dsInfo, CaseSensitive)
            qs.execQuery(pgu, copyInfo.datasetInfo, analysis, false, NoPrecondition, None) match {
              case QueryServer.Success(schema, version, results, etag, lastModified) =>
                (schema, version, results)
            }
          }
        for (result <- mresult) {
          result.foreach { row =>
            println(row.toString())
            row.toString should be("{2=SoQLNumber(0),1=SoQLText(Hello World)}")
          }
        }
      }
    }
  }
}

object QueryTest {

  private val config = PGSecondaryUtil.config

  private val datasourceConfig = new DataSourceConfig(config, "database")
  private val ds = DataSourceFromConfig(datasourceConfig)

}