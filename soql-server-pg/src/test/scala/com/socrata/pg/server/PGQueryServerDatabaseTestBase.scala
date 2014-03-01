package com.socrata.pg.server

import com.rojoma.json.ast.JObject
import com.rojoma.json.util.JsonUtil
import com.socrata.pg.store._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.SoQLAnalysis
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.datacoordinator.common.DataSourceConfig
import org.scalatest.{BeforeAndAfterAll, Matchers}
import scala.language.existentials

trait PGQueryServerDatabaseTestBase extends DatabaseTestBase with PGSecondaryUniverseTestBase {
  this : Matchers with BeforeAndAfterAll =>

  import PGQueryServerDatabaseTestBase._

  val dcInstance = "alpha"

  val project: String = "soql-server-pg"

  val storeId: String = "pg"

  def compareSoqlResult(soql: String, expectedFixture: String) {
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
        val (qrySchema, mresult) =  qs.execQuery(pgu, copyInfo.datasetInfo, analysis)
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
            val next = remainingResult.next
            next should be (expectedRow)
            remainingResult
          }
          whatLeft.hasNext should be (false)
        }
      }
    }
  }
}

object PGQueryServerDatabaseTestBase {

  private val config = PGSecondaryUtil.config

  private val datasourceConfig = new DataSourceConfig(config, "test-database")

  private val qs = new QueryServer(datasourceConfig)
}
