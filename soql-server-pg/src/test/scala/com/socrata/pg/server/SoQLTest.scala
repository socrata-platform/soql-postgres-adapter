package com.socrata.pg.server

import java.util.UUID

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

  private def getContext(secondaryDatasetId: DatasetId): DatasetContext[SoQLType] = withDb { conn =>
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

  val uniquifier = UUID.randomUUID().toString.toLowerCase

  lazy val plainCtx = Map(TableName(s"_manufacturer_${uniquifier}").qualifier -> joinCtx2,
                          TableName(s"_classification_${uniquifier}").qualifier -> joinCtx3,
                          TableName(s"_country_${uniquifier}").qualifier -> joinCtx4)
  lazy val aliasCtx = plainCtx ++ Map(TableName(s"_manufacturer_${uniquifier}", Some("_m")).qualifier -> joinCtx2,
                                      TableName(s"_manufacturer_${uniquifier}", Some("_m2")).qualifier -> joinCtx2,
                                      TableName(s"_manufacturer_${uniquifier}", Some("_z$")).qualifier -> joinCtx2) ++
                                  Map(TableName(s"_classification_${uniquifier}", Some("_c")).qualifier -> joinCtx3,
                                      TableName(s"_classification_${uniquifier}", Some("_c2")).qualifier -> joinCtx3) ++
                                  Map(TableName(s"_country_${uniquifier}", Some("_co")).qualifier -> joinCtx4)

  override def beforeAll: Unit = {
    createDatabases()
    withDb { conn =>
      val (truthDsId2, secDsId2) = importDataset(conn, "mutate-create-2nd-dataset.json", forceUnique = Some(uniquifier))
      val (truthDsId3, secDsId3) = importDataset(conn, "mutate-create-3rd-dataset.json", forceUnique = Some(uniquifier))
      val (truthDsId4, secDsId4) = importDataset(conn, "mutate-create-4th-dataset.json", forceUnique = Some(uniquifier))

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
    withPgu { pgu =>
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
