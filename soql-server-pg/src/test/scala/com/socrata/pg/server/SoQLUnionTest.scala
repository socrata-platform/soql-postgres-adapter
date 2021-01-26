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
   // soql = "SELECT name, breed, age, specie |> SELECT name, breed UNION SELECT @d1.name, @d1.breed from @dog as d1"
    soql = "SELECT name, breed, age, specie, point |> SELECT name, breed, point UNION SELECT @dog.name, @dog.breed, @dog.point from @dog"
    soql = "SELECT name || 'x' as name2 |> SELECT name2 || 'y' as xx" // FAILED
    soql = "SELECT name || 'x' as name2 |> SELECT name2" // OK
    soql = "SELECT name || 'x' as name2 |> SELECT name2 || 'y'" // ?
    soql = "SELECT name, breed limit 2 UNiON all SELECT @dog.name, @dog.breed from @dog"
    soql = "SELECT name, breed, age, specie join (SELECT name, breed FROM @dog |> SELECT name) as j1 on @j1.name=name"
    soql = "SELECT name, breed, age, specie join (SELECT @dog.name, @dog.breed FROM @dog |> SELECT name) as j1 on @j1.name=name"
    soql = "SELECT name join (SELECT @dog.name FROM @dog union SELECT @bird.name FROM @bird) as j1 on @j1.name=name"
    soql = "SELECT name join (SELECT @d1.name FROM @dog as d1 union SELECT @b1.breed FROM @bird as b1) as j1 on @j1.name=name"
    soql = "SELECT name union SELECT @dog.name FROM @dog "
    soql = "SELECT name |> SELECT name as name2"
    soql = "SELECT name, @j1.breed join (select name, breed from @dog) as j1 on @j1.name=name"
    soql = "SELECT name, @j1.breed join (select @d1.name, @d1.breed from @dog as d1) as j1 on @j1.name=name"

    soql = "SELECT name"
    soql = "SELECT name, @dog.breed join @dog on true"
 //   soql = "SELECT name, @d1.breed as breed2 join @dog as d1 on true"
   // soql = "SELECT name, @j1.breed as breed4 join (select @d1.breed from @dog as d1) as j1 on true"
   // soql = "SELECT name, @j1.breed as breed3 join (select breed from @dog) as j1 on true"
    //soql = "SELECT name, @j1.breed as breed5 join (select @dog.breed from @dog) as j1 on true"

    soql = "SELECT name as name2 |> SELECT name2 as name3, @j1.breed as breed3 join (select name,breed from @dog union select @b1.name,@b1.breed from @bird as b1) as j1 on @j1.name=name2 |> select name3, breed3 union select 'name3', 'breed3'" // OK
    //soql = "SELECT name, breed UNION SELECT @d1.name, @d1.breed from @dog as d1 UNION SELECT @bird.name, @bird.breed from @bird"
    //soql = "SELECT name |> SELECT name as name2  |> SELECT name2 as name3"
    //
    // SELECT t1_1.u_name_4 as "name" FROM t1_1 - simplest
    // SELECT t1_1.u_name_4 as "name" FROM t1_1 UNION SELECT t2_1.u_name_4 FROM t2_1
    // SELECT t1_1.u_name_4,t1_1.u_breed_5,t1_1.u_age_7,t1_1.u_specie_6 FROM t1_1 JOIN (SELECT _dog.u_name_4 as "name",_dog.u_breed_5 as "breed" FROM t2_1) as _j1 ON (_j1."name" = t1_1.u_name_4)
    secDatasetId = secDatasetIdCat
    val expectedRowCount: Option[Long] = None
    val caseSensitivity: CaseSensitivity = CaseSensitive
    val joinDatasetCtx: Map[String, DatasetContext[SoQLType]] = plainCtx
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


        ///// SoQL STRING TO ANALYSIS
        var analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = null
        // analyses = SoQLAnalyzerHelper.analyzeSoQLUnion(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)
        //analyses = SoQLAnalyzerHelper.analyzeSoQL(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)
        var banalysis = SoQLAnalyzerHelper.analyzeSoQLBinary(soql, allDatasetCtx, primaryTableColumnNameIdMap ++ joinTableColumnNameIdMap)

        println("analyses done new")

        ///// ANALYSIS TO SQL ONLY

        if (true) {
          ds.run { dsInfo =>
            val qs = new QueryServer(dsInfo, caseSensitivity, leadingSearch)
            //qs.execQueryUnion(pgu, copyInfo.datasetInfo, analyses)
            val sql = qs.execQueryBinary(pgu, copyInfo.datasetInfo, banalysis)
            println("SoQL: " + soql)
            println("SQL : " + sql)
          }
        }

        ///// ANALYSIS TO SQL TO ROWS
        if (false) {
          val (qrySchema, dataVersion, mresult) =
            ds.run { dsInfo =>
              val qs = new QueryServer(dsInfo, caseSensitivity, leadingSearch)
              qs.execQuery(pgu, "someDatasetInternalName", copyInfo.datasetInfo, banalysis, expectedRowCount.isDefined, None, None, true,
                NoPrecondition, None, None, None, false, false, false) match {
                case QueryServer.Success(schema, _, version, results, etag, lastModified) =>
                  (schema, version, results)
                case queryFail: QueryServer.QueryResult =>
                  throw new Exception(s"Query Fail ${queryFail.getClass.getName}")
              }
            }

          val jsonReps = PostgresUniverseCommon.jsonReps(copyInfo.datasetInfo, true)

          val qryReps = qrySchema.mapValues(cinfo => jsonReps(cinfo.typ))

          for (result <- mresult) {
            val listResult = result.toList
            val resultJo = listResult.map { row =>
              val rowJson = qryReps.map { case (cid, rep) =>
                (qrySchema(cid).userColumnId.underlying -> rep.toJValue(row(cid)))
              }
              val jo = JObject(rowJson)
              println(jo)
            }
          }
        }
      }
    }
  }
}
