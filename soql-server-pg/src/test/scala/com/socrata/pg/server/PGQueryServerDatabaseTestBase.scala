package com.socrata.pg.server

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.pg.store._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.SoQLAnalysis
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.datacoordinator.common.{DataSourceFromConfig, DataSourceConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers}
import scala.language.existentials
import com.socrata.pg.soql.{CaseSensitive, CaseSensitivity}
import com.socrata.http.server.util.NoPrecondition
import com.typesafe.config.{ConfigFactory, Config}

trait PGQueryServerDatabaseTestBase extends DatabaseTestBase with PGSecondaryUniverseTestBase {
  this : Matchers with BeforeAndAfterAll =>

  private lazy val datasourceConfig = new DataSourceConfig(config, "database")

  protected lazy val ds = DataSourceFromConfig(datasourceConfig)

  def compareSoqlResult(soql: String, expectedFixture: String, expectedRowCount: Option[Long] = None, caseSensitivity: CaseSensitivity = CaseSensitive) {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon)
      val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secDatasetId).get)
      for (readCtx <- pgu.datasetReader.openDataset(copyInfo)) yield {
        val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
        val columnNameTypeMap: OrderedMap[ColumnName, SoQLType] = baseSchema.values.foldLeft(OrderedMap.empty[ColumnName, SoQLType]) { (map, cinfo) =>
          map + (ColumnName(cinfo.userColumnId.underlying) -> cinfo.typ)
        }
        val datasetCtx = new DatasetContext[SoQLType] {
          val schema = columnNameTypeMap
        }
        val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
        val (qrySchema, dataVersion, mresult) =
          for (dsInfo <- ds) yield {
            val qs = new QueryServer(dsInfo, caseSensitivity)
            qs.execQuery(pgu, "someDatasetInternalName", copyInfo.datasetInfo, analysis, expectedRowCount.isDefined, None, None, NoPrecondition, None) match {
              case QueryServer.Success(schema, _, version, results, etag, lastModified) =>
                (schema, version, results)
              case queryFail =>
                throw new Exception(s"Query Fail ${queryFail.getClass.getName}")
            }
          }
        val jsonReps = PostgresUniverseCommon.jsonReps(copyInfo.datasetInfo)

        val qryReps = qrySchema.mapValues( cinfo => jsonReps(cinfo.typ))

        for (result <- mresult) {
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
            next should be (expectedRow)
            remainingResult
          }

          whatLeft.hasNext should be (false)
          // check row count
          result.rowCount should be (expectedRowCount)
        }
      }
    }
  }
}
