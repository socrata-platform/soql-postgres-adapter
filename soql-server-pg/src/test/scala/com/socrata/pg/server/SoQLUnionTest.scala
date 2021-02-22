package com.socrata.pg.server

import com.socrata.datacoordinator.id.{DatasetId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.store.{PGSecondaryTestBase, PGSecondaryUniverse, PostgresUniverseCommon}
import com.socrata.pg.query.PGQueryTestBase
import com.socrata.pg.soql.SqlizerTest.sqlCtx
import com.socrata.pg.soql.{BinarySoQLAnalysisSqlizer, CaseSensitive, ParametricSql, QualifiedUserColumnId, SqlColIdx, SqlizerContext}
import com.socrata.soql.analyzer.{QualifiedColumnName, SoQLAnalyzerHelper}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
import com.socrata.soql.exceptions.NoSuchColumn
import com.socrata.soql.types.{SoQLType, SoQLValue}

class SoQLUnionTest extends PGSecondaryTestBase with PGQueryServerDatabaseTestBase with PGQueryTestBase {

  val catTable = TableName("_cat")
  val dogTable = TableName("_dog")
  val birdTable = TableName("_bird")
  val fishTable = TableName("_fish")

  var truthDatasetIdCat = DatasetId.Invalid
  var secDatasetIdCat = DatasetId.Invalid
  var truthDatasetIdDog = DatasetId.Invalid
  var secDatasetIdDog = DatasetId.Invalid
  var truthDatasetIdBird = DatasetId.Invalid
  var secDatasetIdBird = DatasetId.Invalid
  var truthDatasetIdFish = DatasetId.Invalid
  var secDatasetIdFish = DatasetId.Invalid

  private lazy val catCtx = getContext(secDatasetIdCat)
  private lazy val dogCtx = getContext(secDatasetIdDog)
  private lazy val birdCtx = getContext(secDatasetIdBird)
  private lazy val fishCtx = getContext(secDatasetIdFish)

  private lazy val catReps = getColumnReps(secDatasetIdCat)
  private lazy val dogReps = getColumnReps(secDatasetIdDog)
  private lazy val birdReps = getColumnReps(secDatasetIdBird)
  private lazy val fishReps = getColumnReps(secDatasetIdFish)

  private lazy val catColumnMap = getColumnMap(secDatasetIdCat)
  private lazy val dogColumnMap = getColumnMap(secDatasetIdDog)
  private lazy val birdColumnMap = getColumnMap(secDatasetIdBird)
  private lazy val fishColumnMap = getColumnMap(secDatasetIdFish)

  private lazy val typeReps = getTypeReps(secDatasetIdCat)

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

  private def getColumnMap(secondaryDatasetId: DatasetId): Map[QualifiedColumnName, UserColumnId] = withDb() { conn =>
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
    val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secondaryDatasetId).get)

    pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
      val qualifier = copyInfo.datasetInfo.resourceName
      val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
      baseSchema.values.map { columnInfo =>
        val userColumnId = columnInfo.userColumnId
        QualifiedColumnName(qualifier, ColumnName(userColumnId.underlying)) -> userColumnId
      }
    }.toMap
  }

  private def getColumnReps(secondaryDatasetId: DatasetId): Map[UserColumnId, SqlColIdx] = withDb() { conn =>
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
    val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secondaryDatasetId).get)

    pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
      val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
      val reps = baseSchema.values.toSeq.map { cinfo: ColumnInfo[SoQLType] =>
        cinfo.userColumnId -> PostgresUniverseCommon.repForIndex(cinfo)
      }
      reps.toMap
    }
  }

  private def getTypeReps(secondaryDatasetId: DatasetId): Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]] = withDb() { conn =>
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
    val copyInfo: CopyInfo = pgu.datasetMapReader.latest(pgu.datasetMapReader.datasetInfo(secondaryDatasetId).get)

    pgu.datasetReader.openDataset(copyInfo).run { readCtx =>
      val baseSchema: ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]] = readCtx.schema
      val reps = baseSchema.values.toSeq.map { cinfo => (cinfo.typ -> PostgresUniverseCommon.repForIndex(cinfo)) }
      reps.toMap
    }
  }

  val tableNameMap = Map(TableName.PrimaryTable -> "t1",
                         catTable -> "t1",
                         dogTable -> "t2",
                         birdTable -> "t3",
                         fishTable -> "t4")

  lazy val plainCtx = Map(TableName.PrimaryTable.qualifier -> catCtx,
                          catTable.qualifier -> catCtx,
                          dogTable.qualifier -> dogCtx,
                          birdTable.qualifier -> birdCtx,
                          fishTable.qualifier -> fishCtx)

  lazy val allReps = (catReps.values ++ dogReps.values ++ birdReps.values ++ fishReps.values).toSeq

  lazy val qualifiedReps =
    catReps.map { case (k, v) => QualifiedUserColumnId(None, k) -> v } ++
      catReps.map { case (k, v) => QualifiedUserColumnId(Some(catTable.qualifier), k) -> v } ++
      dogReps.map { case (k, v) => QualifiedUserColumnId(Some(dogTable.qualifier), k) -> v } ++
      birdReps.map { case (k, v) => QualifiedUserColumnId(Some(birdTable.qualifier), k) -> v } ++
      fishReps.map { case (k, v) => QualifiedUserColumnId(Some(fishTable.qualifier), k) -> v }

  lazy val columnMap: Map[QualifiedColumnName, UserColumnId] =
    catColumnMap.map { case (QualifiedColumnName(_, name), v) => (QualifiedColumnName(None, name), v) } ++
    catColumnMap ++ dogColumnMap ++ birdColumnMap ++ fishColumnMap

  override def beforeAll: Unit = {
    createDatabases()
    withDb() { conn =>
      val (truthCatId1, secCatId1) = importDataset(conn, "mutate-create-cat-dataset.json")
      val (truthDogId2, secDogId2) = importDataset(conn, "mutate-create-dog-dataset.json")
      val (truthBirdId3, secBirdId3) = importDataset(conn, "mutate-create-bird-dataset.json")
      val (truthFishId4, secFishId4) = importDataset(conn, "mutate-create-fish-dataset.json")

      truthDatasetIdCat = truthCatId1
      secDatasetIdCat = secCatId1

      truthDatasetIdDog = truthDogId2
      secDatasetIdDog = secDogId2

      truthDatasetIdBird = truthBirdId3
      secDatasetIdBird = secBirdId3

      truthDatasetIdFish = truthFishId4
      secDatasetIdFish = secFishId4
    }
  }

  override def afterAll: Unit = {
    withPgu() { pgu =>

      Seq(truthDatasetIdCat, truthDatasetIdDog, truthDatasetIdBird, truthDatasetIdFish).foreach { dsId =>
        if (dsId != DatasetId.Invalid) {
          dropDataset(pgu, dsId)
        }
      }
      cleanupDroppedTables(pgu)
    }
    super.afterAll
  }

  val soqls = Map(
    "union no table alias" -> "SELECT name WHERE 3 <> 2 UNION select name FROM @dog WHERE year=1 |> SELECT name order by name",
    "union table alias" -> "SELECT name UNION select @d1.name FROM @dog as d1 group by @d1.name |> SELECT name order by name",
    "urls" -> "SELECT url, cat WHERE url is not null UNION SELECT url, dog FROM @dog WHERE url is not null |> SELECT url ORDER BY cat",
    "mixed and nested" ->
      """
      SELECT name, @jd1.breed, @jd1.dog, @dog.breed as b2, @jb1.breed as b3, @jb1.bird
        JOIN (SELECT @dog.breed, dog, year FROM @dog) as jd1 on @jd1.year=year
        JOIN @dog on @dog.year=year
        JOIN (SELECT @bird.breed,bird,year FROM @bird WHERE year=1 AND @bird.year+1=2 UNION
             (SELECT @fish.breed,fish,1 FROM @fish WHERE year=2 GROUP BY @fish.breed,fish UNION
              SELECT @jb1.breed, bird,1 FROM @bird JOIN @bird as jb1 on year=@jb1.year WHERE year=3)
             ) as jb1 on @jb1.year=year
       WHERE year=1
      |> SELECT name, breed, b3, bird as type ORDER BY b3, bird LIMIT 5
      """
  )

  test("Result set union no table alias") {
    val soql = soqls("union no table alias")
    compareSoqlResult(soql, "union-no-table-alias.json", None, CaseSensitive, plainCtx, secDatasetId = secDatasetIdCat)
  }

  test("Result set union table alias") {
    val soql = soqls("union table alias")
    compareSoqlResult(soql, "union-table-alias.json", None, CaseSensitive, plainCtx, secDatasetId = secDatasetIdCat)
  }

  test("Result set union urls") {
    val soql = soqls("urls")
    compareSoqlResult(soql, "union-urls.json", None, CaseSensitive, plainCtx, secDatasetId = secDatasetIdCat)
  }

  test("Result set mixed and nested") {
    val soql = soqls("mixed and nested")
    compareSoqlResult(soql, "union-mixed-and-nested.json", None, CaseSensitive, plainCtx, secDatasetId = secDatasetIdCat)
  }

  test("Sqlize - union no table alias") {
    sqlizeTest(soqls("union no table alias"), """SELECT "name" FROM (SELECT t1.u_name_4 as "name" FROM t1 WHERE (? != ?) UNION SELECT t2.u_name_4 as "name" FROM t2 WHERE (t2.u_year_6 = ?)) AS x1 ORDER BY "name" nulls last""",
      Seq(3, 2, 1))
  }

  test("Sqlize - union table alias") {
    sqlizeTest(soqls("union table alias"), """SELECT "name" FROM (SELECT t1.u_name_4 as "name" FROM t1 UNION SELECT _d1.u_name_4 as "name" FROM t2 as _d1 GROUP BY _d1.u_name_4) AS x1 ORDER BY "name" nulls last""")
  }

  test("Sqlize - urls") {
    sqlizeTest(soqls("urls"), """SELECT "url_url","url_description" FROM (SELECT t1.u_url_8_url as "url_url",t1.u_url_8_description as "url_description",t1.u_cat_7 as "cat" FROM t1 WHERE (t1.u_url_8_url is not null or t1.u_url_8_description is not null) UNION SELECT t2.u_url_8_url as "url_url",t2.u_url_8_description as "url_description",t2.u_dog_7 as "dog" FROM t2 WHERE (t2.u_url_8_url is not null or t2.u_url_8_description is not null)) AS x1 ORDER BY "cat" nulls last""")
  }

  test("Sqlize - mixed and nested") {
    sqlizeTest(soqls("mixed and nested"), """SELECT "name","breed","b3","bird" FROM (SELECT t1.u_name_4 as "name",_jd1."breed" as "breed",_jd1."dog" as "dog",t2.u_breed_5 as "b2",_jb1."breed" as "b3",_jb1."bird" as "bird" FROM t1 JOIN (SELECT t2.u_breed_5 as "breed",t2.u_dog_7 as "dog",t2.u_year_6 as "year" FROM t2) as _jd1 ON (_jd1."year" = t1.u_year_6)  JOIN t2 ON (t2.u_year_6 = t1.u_year_6)  JOIN (SELECT t3.u_breed_5 as "breed",t3.u_bird_8 as "bird",t3.u_year_6 as "year" FROM t3 WHERE ((t3.u_year_6 = ?) and ((t3.u_year_6 + ?) = ?)) UNION (SELECT t4.u_breed_5 as "breed",t4.u_fish_8 as "fish",? as "_1" FROM t4 WHERE (t4.u_year_6 = ?) GROUP BY t4.u_breed_5,t4.u_fish_8 UNION SELECT _jb1.u_breed_5 as "breed",t3.u_bird_8 as "bird",? as "_1" FROM t3 JOIN t3 as _jb1 ON (t3.u_year_6 = _jb1.u_year_6) WHERE (t3.u_year_6 = ?))) as _jb1 ON (_jb1."year" = t1.u_year_6) WHERE (t1.u_year_6 = ?)) AS x1 ORDER BY "b3" nulls last,"bird" nulls last LIMIT 5""",
      Seq(1, 1, 2, 1, 2, 1, 3, 1))
  }

  test("Union rows_count") {
    val soql = "SELECT name UNION SELECT name FROM @dog UNION ALL SELECT name FROM @bird UNION SELECT name FROM @fish |> SELECT name ORDER BY name OFFSET 2 LIMIT 2"
    compareSoqlResult(soql, "union-rows-count.json", expectedRowCount = Some(12), CaseSensitive, plainCtx, secDatasetId = secDatasetIdCat)
  }

  test("Sqlize - cannot select column in the other part of an union") {
    val err = intercept[NoSuchColumn] {
      sqlizeTest("SELECT name UNION SELECT cat FROM @fish", "")
    }
    err.getMessage.contains("cat") should be(true)
  }

  def sqlizeTest(soql: String, expected: String, expectedParams: Seq[Any] = Seq.empty): Unit = {
    val ParametricSql(Seq(sql), setParams) = sqlize(soql,
                                            plainCtx,
                                            columnMap,
                                            tableNameMap,
                                            allReps,
                                            qualifiedReps,
                                            typeReps)
    val actualParams = setParams.map(setParam => setParam(None, 0).get)
    actualParams should be(expectedParams)
    sql should be(expected)
  }

  private val passThrough = (s: String) => s

  def sqlize(soql: String,
             datasetCtx: Map[String, DatasetContext[SoQLType]],
             idMap: Map[QualifiedColumnName, UserColumnId],
             tableMap: Map[TableName, String],
             allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]],
             rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]]): ParametricSql = {
    val analyses = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    BinarySoQLAnalysisSqlizer.sql((analyses, tableMap, allColumnReps))(
      rep,
      typeRep,
      Seq.empty,
      sqlCtx + (SqlizerContext.CaseSensitivity -> CaseSensitive),
      passThrough)
  }
}
