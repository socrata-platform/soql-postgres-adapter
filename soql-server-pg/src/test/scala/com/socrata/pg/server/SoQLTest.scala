package com.socrata.pg.server

import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverse, PostgresUniverseCommon}
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.query.PGQueryTestBase
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.types.{SoQLType, SoQLValue}


abstract class SoQLTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  var truthDatasetIdJoin = DatasetId.Invalid
  var secDatasetIdJoin = DatasetId.Invalid

  private lazy val joinCtx = withDb() { conn =>
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
    val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secDatasetIdJoin).get)

    pgu.datasetReader.openDataset(copyInfo).map { readCtx =>
      val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
      val columnNameTypeMap: OrderedMap[ColumnName, SoQLType] = baseSchema.values.foldLeft(OrderedMap.empty[ColumnName, SoQLType]) { (map, cinfo) =>
        map + (ColumnName(cinfo.userColumnId.underlying) -> cinfo.typ)
      }
      val datasetCtx = new DatasetContext[SoQLType] {
        val schema = columnNameTypeMap
      }
      datasetCtx
    }
  }

  lazy val plainCtx = Map(TableName("_manufacturer").qualifier -> joinCtx)
  lazy val aliasCtx = Map(TableName("_manufacturer", Some("_m")).qualifier -> joinCtx)

  override def beforeAll: Unit = {
    createDatabases()
    withDb() { conn =>
      val (truthDsId, secDsId) = importDataset(conn, "mutate-create-2nd-dataset.json")
      truthDatasetIdJoin = truthDsId
      secDatasetIdJoin = secDsId
      importDataset(conn)
    }
  }

  override def afterAll: Unit = {
    withPgu() { pgu =>
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val tableName = pgu.datasetMapReader.latest(datasetInfo).dataTableName
      dropDataset(pgu, truthDatasetId)
      if (truthDatasetIdJoin != DatasetId.Invalid) {
        dropDataset(pgu, truthDatasetIdJoin)
      }
      cleanupDroppedTables(pgu)
      hasDataTables(pgu.conn, tableName, datasetInfo) should be (false)
    }
    super.afterAll
  }
}
