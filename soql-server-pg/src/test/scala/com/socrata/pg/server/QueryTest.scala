package com.socrata.pg.server

import com.socrata.datacoordinator.id.{RowId, UserColumnId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store._
import com.socrata.soql.analyzer.{QualifiedColumnName, SoQLAnalyzerHelper}
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.types.{SoQLID, SoQLType}
import com.socrata.soql.collection.OrderedMap
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import java.sql.Connection

import org.apache.log4j.PropertyConfigurator

import scala.language.existentials
import com.socrata.pg.soql.CaseSensitive
import com.socrata.http.server.util.NoPrecondition
import com.socrata.pg.query.PGQueryTestBase

class QueryTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  override def beforeAll: Unit = {
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
      dummyVals filterKeys (!UnsupportedTypes.contains(_)) foreach {
        (v) => assert(rowValues.contains(v._2), "Could not find " + v + " in row values: " + rowValues)
      }

      val idMap =  (cn: ColumnName) => new UserColumnId(cn.name)
      val soql = "select text_USERNAME, number_USERNAME"

      pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
        val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
        val columnNameTypeMap: OrderedMap[ColumnName, SoQLType] = baseSchema.values.foldLeft(OrderedMap.empty[ColumnName, SoQLType]) { (map, cinfo) =>
          map + (ColumnName(cinfo.userColumnId.underlying) -> cinfo.typ)
        }
        val datasetCtx = new DatasetContext[SoQLType] {
          val schema = columnNameTypeMap
        }

        val primaryTableColumnNameIdMap = columnNameTypeMap.map { case (columnName, _) =>
          QualifiedColumnName(None, columnName) -> idMap(columnName)
        }

        val analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] =
          SoQLAnalyzerHelper.analyzeSoQL(soql, Map(TableName.PrimaryTable.qualifier -> datasetCtx), primaryTableColumnNameIdMap)
        val (requestColumns, version, mresult) =
          ds.run { dsInfo =>
            val qs = new QueryServer(dsInfo, CaseSensitive, leadingSearch = true)
            qs.execQuery(pgu, Map.empty, "someDatasetInternalName", copyInfo.datasetInfo, analyses, false, None, None, true,
              NoPrecondition, None, None, None, false, false, false) match {
              case QueryServer.Success(schema, _, version, results, etag, lastModified) =>
                (schema, version, results)
              case queryFail: QueryServer.QueryResult =>
                throw new Exception(s"Query Fail ${queryFail.getClass.getName}")
            }
          }

        mresult.map { result =>
          result.foreach { row =>
            row.toString should be("{2=SoQLNumber(0),1=SoQLText(Hello World)}")
          }
        }
      }
    }
  }
}
