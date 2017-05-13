package com.socrata.pg.server

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.rojoma.json.v3.ast.{JArray, JNumber, JObject, JValue}
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.server.util.NoPrecondition
import com.socrata.pg.soql.{CaseSensitive, CaseSensitivity}
import com.socrata.pg.store._
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType, SoQLValue}
import org.scalatest.matchers.{BeMatcher, MatchResult}

import scala.language.existentials

// scalastyle:off method.length
trait PGQueryServerDatabaseTestBase extends DatabaseTestBase with PGSecondaryUniverseTestBase {
  private lazy val datasourceConfig = new DataSourceConfig(config, "database")

  protected lazy val ds = DataSourceFromConfig(datasourceConfig)

  def compareSoqlResult(soql: String,
                        expectedFixture: String,
                        expectedRowCount: Option[Long] = None,
                        caseSensitivity: CaseSensitivity = CaseSensitive,
                        joinDatasetCtx: Map[String, DatasetContext[SoQLType]] = Map.empty): Unit = {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon)
      val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secDatasetId).get)

      pgu.datasetReader.openDataset(copyInfo).map { readCtx =>
        val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
        val columnNameTypeMap: OrderedMap[ColumnName, SoQLType] = baseSchema.values.foldLeft(OrderedMap.empty[ColumnName, SoQLType]) { (map, cinfo) =>
          map + (ColumnName(cinfo.userColumnId.underlying) -> cinfo.typ)
        }
        val datasetCtx = new DatasetContext[SoQLType] {
          val schema = columnNameTypeMap
        }

        val columnNameIdMap = columnNameTypeMap.map { case (columnName, typ) =>
          columnName -> idMap(columnName)
        }

        val allDatasetCtx = joinDatasetCtx + (TableName.PrimaryTable.qualifier -> datasetCtx)
        val analyses: Seq[SoQLAnalysis[UserColumnId, SoQLType]] =
          SoQLAnalyzerHelper.analyzeSoQL(soql, allDatasetCtx, columnNameIdMap)

        val (qrySchema, dataVersion, mresult) =
          ds.map { dsInfo =>
            val qs = new QueryServer(dsInfo, caseSensitivity)
            qs.execQuery(pgu, "someDatasetInternalName", copyInfo.datasetInfo, analyses, expectedRowCount.isDefined, None, None, true,
              NoPrecondition, None, None, None) match {
              case QueryServer.Success(schema, _, version, results, etag, lastModified) =>
                (schema, version, results)
              case queryFail: QueryServer.QueryResult =>
                throw new Exception(s"Query Fail ${queryFail.getClass.getName}")
            }
          }
        val jsonReps = PostgresUniverseCommon.jsonReps(copyInfo.datasetInfo, true)

        val qryReps = qrySchema.mapValues( cinfo => jsonReps(cinfo.typ))

        mresult.map { result =>
          val resultJo = result.map { row =>
            val rowJson = qryReps.map { case (cid, rep) =>
              (qrySchema(cid).userColumnId.underlying -> rep.toJValue(row(cid)))
            }
            JObject(rowJson)
          }
          val expected = fixtureRows(expectedFixture)
          val whatLeft = expected.foldLeft(resultJo) { (remainingResult, expectedRow ) =>
            remainingResult.hasNext should be (true)
            val next = remainingResult.next
            next should be (approximatelyTheSameAs(expectedRow))
            remainingResult
          }

          whatLeft.hasNext should be (false)
          // check row count
          result.rowCount should be (expectedRowCount)
        }
      }
    }
  }

  def approximatelyTheSameAs(expected: JValue) = new BeMatcher[JValue] {
    override def apply(got: JValue): MatchResult =
      MatchResult(approximatelyEqual(got, expected), got + " did not (approximately) equal " + expected, got + " (approximately) equalled " + expected)

    private def approximatelyEqual(got: JValue, expected: JValue): Boolean = (got, expected) match {
      case (gotN: JNumber, expectedN: JNumber) =>
        // ok, here's the special case.  We don't want to be sensitive to changes in floating point
        // values (this has happened before when PostGIS changed its distance algorithm) so, since
        // we don't actually want to test that the functions do what they say they do, only that we're
        // calling the right ones, we'll call these "equal" if "got" is within 0.001% of "expected".
        val diff = (gotN.toBigDecimal - expectedN.toBigDecimal).abs
        diff <= 0.00001 * expectedN.toBigDecimal.abs
      case (JArray(gotA), JArray(expectedA)) =>
        gotA.length == expectedA.length && (gotA, expectedA).zipped.forall(approximatelyEqual)
      case (JObject(gotO), JObject(expectedO)) =>
        gotO.keySet == expectedO.keySet && gotO.keySet.forall { k => approximatelyEqual(gotO(k), expectedO(k)) }
      case (g, e) =>
        g == e
    }
  }
}
