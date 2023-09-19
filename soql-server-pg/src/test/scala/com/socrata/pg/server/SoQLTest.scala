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

  var truthDatasetIdJoin2 = DatasetId.Invalid
  var secDatasetIdJoin2 = DatasetId.Invalid
  var truthDatasetIdJoin3 = DatasetId.Invalid
  var secDatasetIdJoin3 = DatasetId.Invalid
  var truthDatasetIdJoin4 = DatasetId.Invalid
  var secDatasetIdJoin4 = DatasetId.Invalid


  private lazy val joinCtx2 = getContext(secDatasetIdJoin2)
  private lazy val joinCtx3 = getContext(secDatasetIdJoin3)
  private lazy val joinCtx4 = getContext(secDatasetIdJoin4)

  private def getContext(secondaryDatasetId: DatasetId): DatasetContext[SoQLType] = withDbUnconstrained() { conn =>
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
    val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secondaryDatasetId).get)

    pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
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

  lazy val plainCtx = Map(TableName("_manufacturer").qualifier -> joinCtx2,
                          TableName("_classification").qualifier -> joinCtx3,
                          TableName("_country").qualifier -> joinCtx4)
  lazy val aliasCtx = plainCtx ++ Map(TableName("_manufacturer", Some("_m")).qualifier -> joinCtx2,
                                      TableName("_manufacturer", Some("_m2")).qualifier -> joinCtx2,
                                      TableName("_manufacturer", Some("_z$")).qualifier -> joinCtx2) ++
                                  Map(TableName("_classification", Some("_c")).qualifier -> joinCtx3,
                                      TableName("_classification", Some("_c2")).qualifier -> joinCtx3) ++
                                  Map(TableName("_country", Some("_co")).qualifier -> joinCtx4)

  override def beforeAll: Unit = {
    createDatabases()
    withDbUnconstrained() { conn =>
      val (truthDsId2, secDsId2) = importDataset(conn, "mutate-create-2nd-dataset.json")
      val (truthDsId3, secDsId3) = importDataset(conn, "mutate-create-3rd-dataset.json")
      val (truthDsId4, secDsId4) = importDataset(conn, "mutate-create-4th-dataset.json")

      truthDatasetIdJoin2 = truthDsId2
      secDatasetIdJoin2 = secDsId2

      truthDatasetIdJoin3 = truthDsId3
      secDatasetIdJoin3 = secDsId3

      truthDatasetIdJoin4 = truthDsId4
      secDatasetIdJoin4 = secDsId4

      importDataset(conn)
    }
  }

  override def afterAll: Unit = {
    withPguUnconstrained() { pgu =>
      val datasetInfo = pgu.datasetMapReader.datasetInfo(secDatasetId).get
      val tableName = pgu.datasetMapReader.latest(datasetInfo).dataTableName
      dropDataset(pgu, truthDatasetId)
      Seq(truthDatasetIdJoin2, truthDatasetIdJoin3).foreach { dsId =>
        if (dsId != DatasetId.Invalid) {
          dropDataset(pgu, dsId)
        }
      }
      cleanupDroppedTables(pgu)
      hasDataTables(pgu.conn, tableName, datasetInfo) should be (false)
    }
    super.afterAll
  }
}
