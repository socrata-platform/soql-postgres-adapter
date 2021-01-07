package com.socrata.pg.server

import com.rojoma.json.v3.ast.JObject
import com.socrata.NonEmptySeq
import com.socrata.datacoordinator.id.{DatasetId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.server.util.NoPrecondition
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverse, PostgresUniverseCommon}
import com.socrata.pg.store.PGSecondaryUtil._
import com.socrata.pg.query.PGQueryTestBase
import com.socrata.pg.soql.{CaseSensitive, CaseSensitivity}
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.analyzer.{QualifiedColumnName, SoQLAnalyzerHelper}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.{Parser, StandaloneParser}
import com.socrata.soql.types.{SoQLType, SoQLValue}


class SoQLUnionTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  var truthDatasetIdCat = DatasetId.Invalid
  var secDatasetIdCat = DatasetId.Invalid
  var truthDatasetIdDog = DatasetId.Invalid
  var secDatasetIdDog = DatasetId.Invalid
  var truthDatasetIdBird = DatasetId.Invalid
  var secDatasetIdBird = DatasetId.Invalid


  private lazy val birdCtx = getContext(secDatasetIdBird)
  private lazy val catCtx = getContext(secDatasetIdCat)
  private lazy val dogCtx = getContext(secDatasetIdDog)

  private def getContext(secondaryDatasetId: DatasetId): DatasetContext[SoQLType] = withDb() { conn =>
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

  lazy val plainCtx = Map(TableName("_").qualifier -> catCtx,
    TableName("_dog").qualifier -> dogCtx,
    TableName("_bird").qualifier -> birdCtx)
  lazy val aliasCtx = plainCtx ++ Map(
    TableName("_dog", Some("_d1")).qualifier -> dogCtx,
    TableName("_bird", Some("_b1")).qualifier -> birdCtx)

  override def beforeAll: Unit = {
    createDatabases()
    withDb() { conn =>
      val (truthCatId2, secCatId2) = importDataset(conn, "mutate-create-cat-dataset.json")
      val (truthDogId3, secDogId3) = importDataset(conn, "mutate-create-dog-dataset.json")
      val (truthBirdId4, secBirdId4) = importDataset(conn, "mutate-create-bird-dataset.json")

      truthDatasetIdCat = truthCatId2
      secDatasetIdCat = secCatId2

      truthDatasetIdDog = truthDogId3
      secDatasetIdDog = secDogId3

      truthDatasetIdBird = truthBirdId4
      secDatasetIdBird = secBirdId4

      importDataset(conn)
    }
  }

  override def afterAll: Unit = {
    withPgu() { pgu =>

      Seq(truthDatasetIdCat, truthDatasetIdDog, truthDatasetIdBird).foreach { dsId =>
        if (dsId != DatasetId.Invalid) {
          dropDataset(pgu, dsId)
        }
      }
      cleanupDroppedTables(pgu)
    }
    super.afterAll
  }

  test("union 1") {
    println("hello")
    var soql = "SELECT name, @d1.breed JOIN @dog as d1 ON name=@d1.name |> select name, breed, 'hi'"
    soql = "SELECT name, breed, age, specie, point |> SELECT name, breed, point |> SELECT point"
    soql = "SELECT name, `union`, breed, point UNION select @d1.name, @d1.`union`, point from @dog as d1"
   // soql = "SELECT name, breed UNION SELECT @d1.name, @d1.breed from @dog as d1 UNION SELECT @b1.name, @b1.breed from @bird as b1"
   // soql = "SELECT name, breed UNION SELECT @dog.name, @dog.breed from @dog"
   // soql = "SELECT name, breed, age, specie |> SELECT name, breed UNION SELECT @d1.name, @d1.breed from @dog as d1"
    soql = "SELECT name, breed, age, specie, point |> SELECT name, breed, point UNION SELECT @dog.name, @dog.breed, @dog.point from @dog"
    // soql = "SELECT name, breed, @dog.name as dogname JOIN @dog on @dog.name=name |> select name, dogname"
    secDatasetId = secDatasetIdCat
    val expectedRowCount: Option[Long] = None
    val caseSensitivity: CaseSensitivity = CaseSensitive
    val joinDatasetCtx: Map[String, DatasetContext[SoQLType]] = aliasCtx
    val leadingSearch: Boolean = true

    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon)
      val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secDatasetId).get)

      pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
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

        val primaryTableColumnNameIdMap = columnNameIdMap.map { case (k, v) =>
          QualifiedColumnName(None, k) -> v
        }

        val allDatasetCtx = joinDatasetCtx + (TableName.PrimaryTable.qualifier -> datasetCtx)

        val joinTableColumnNameIdMap = joinDatasetCtx.foldLeft(Map.empty[QualifiedColumnName, UserColumnId]) { (acc, jctx) =>
          val (name, ctx) = jctx
          val columnNameIdMap = ctx.columns.map { columnName =>
            QualifiedColumnName(Some(name), columnName) -> new UserColumnId(columnName.name)
          }
          acc ++ columnNameIdMap
        }

        var analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = null
       // analyses = SoQLAnalyzerHelper.analyzeSoQLUnion(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)
        //analyses = SoQLAnalyzerHelper.analyzeSoQL(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)
        var banalysis = SoQLAnalyzerHelper.analyzeSoQLBinary(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)
        println("analyses done new")

        ds.run { dsInfo =>
          val qs = new QueryServer(dsInfo, caseSensitivity, leadingSearch)
          //qs.execQueryUnion(pgu, copyInfo.datasetInfo, analyses)
          val sql = qs.execQueryBinary(pgu, copyInfo.datasetInfo, banalysis)
          println(sql)
        }


  //      val jsonReps = PostgresUniverseCommon.jsonReps(copyInfo.datasetInfo, true)

//        val qryReps = qrySchema.mapValues( cinfo => jsonReps(cinfo.typ))
//
//        for (result <- mresult) {
//          val resultJo = result.map { row =>
//            val rowJson = qryReps.map { case (cid, rep) =>
//              (qrySchema(cid).userColumnId.underlying -> rep.toJValue(row(cid)))
//            }
//            println(rowJson)
//            //JObject(rowJson)
//          }
//        }
      }
    }
  }
}
