package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers

import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.soql.types._
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, DatabasePopulator => TruthDatabasePopulator}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import org.postgresql.util.PSQLException
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import org.joda.time.{LocalTime, LocalDate, LocalDateTime, DateTime}
import com.rojoma.json.ast.{JArray, JString, JObject}
import java.math.BigDecimal
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo


/**
 *
 */
class PGSecondaryUniverseTest extends FunSuite with MustMatchers with BeforeAndAfterAll {
    type CT = SoQLType
    type CV = SoQLValue
    val common = PostgresUniverseCommon
    override def beforeAll() {
    }

    override def afterAll() {
    }

    def populateDatabase(conn: Connection) {
      val sql = TruthDatabasePopulator.metadataTablesCreate(DatasetMapLimits())
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }

    def withDb[T]()(f: (Connection) => T): T = {
      def loglevel = 0; // 2 = debug, 0 = default
      using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) { conn =>
        conn.setAutoCommit(false)
        populateDatabase(conn)
        f(conn)
      }
    }

    def createTable(conn:Connection):(PGSecondaryUniverse[SoQLType, SoQLValue], CopyInfo, SchemaLoader[SoQLType]) = {
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
      val copyInfo = pgu.datasetMapWriter.create("us") // locale
      val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
      sLoader.create(copyInfo)
      (pgu, copyInfo, sLoader)
    }

    def createTableWithSchema(pgu:PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo:CopyInfo, sLoader:SchemaLoader[SoQLType]) = {
      // Setup the data columns
      val cols = SoQLType.typesByName filterKeys (!Set(TypeName("json")).contains(_)) map {
        case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
      }

      // Set up the required columns
      val systemPrimaryKey:ColumnInfo[SoQLType] =
        pgu.datasetMapWriter.setSystemPrimaryKey(pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(":id"), SoQLID, "system_id"))
      val versionColumn:ColumnInfo[SoQLType] =
        pgu.datasetMapWriter.setVersion(pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(":version"), SoQLVersion, "version_id"))

      // Add all columns
      sLoader.addColumns(cols ++ Seq(systemPrimaryKey, versionColumn))

      // Register the system and version columns
      sLoader.makeSystemPrimaryKey(systemPrimaryKey)
      sLoader.makeVersion(versionColumn)

      // get the schema and validate
      val schema = getSchema(pgu, copyInfo)
      validateSchema(cols, schema)
      schema
    }

    def getSchema(pgu:PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo:CopyInfo):ColumnIdMap[ColumnInfo[SoQLType]] = {
      for (reader <- pgu.datasetReader.openDataset(copyInfo)) yield reader.schema
    }

    def validateSchema(expect:Iterable[ColumnInfo[SoQLType]], schema:ColumnIdMap[ColumnInfo[SoQLType]]) {
      val existing = (schema.values map {
        colInfo => (colInfo.systemId, colInfo.typ)
      }).toMap

      expect foreach {
        colInfo =>  {
          existing must contain (colInfo.systemId, colInfo.typ)
          //println("Checked that " + colInfo.userColumnId+ ":" + colInfo.typeName + " was truly and surely created")
        }
      }
    }

    def jdbcColumnCount(conn:Connection, tableName:String):Integer = {
      val rs = conn.getMetaData.getColumns(null, null, tableName, null)
      rs.last
      rs.getRow
    }

    def insertDummyRow(id:RowId, values:Map[TypeName, SoQLValue], pgu:PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo:CopyInfo, schema:ColumnIdMap[ColumnInfo[SoQLType]]) {
      // Setup our row data with column Ids
      val colIdMap = schema.foldLeft(ColumnIdMap[SoQLValue]())  { (acc, kv) =>
        val (cId, columnInfo) = kv
        acc + (cId -> values(columnInfo.typ.name))
      }

      // Perform the insert
      val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
      val loader = pgu.prevettedLoader(copyCtx, pgu.logger(copyInfo.datasetInfo, "test-user"))

      loader.insert(id, colIdMap)
      loader.flush()
      //pgu.commit()
    }


    def getRow(id:RowId,pgu:PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo:CopyInfo, schema:ColumnIdMap[ColumnInfo[SoQLType]]) = {
      val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
      val reader = pgu.reader(copyCtx)
      reader.lookupRows(Seq(SoQLID(id.underlying)).iterator)
    }

    def getSpecialColumnIds(schema:ColumnIdMap[ColumnInfo[SoQLType]]) = {
      val versionColId = schema.filterNot {
        (colId, colInfo) => colInfo.typ != SoQLVersion
      }.iterator.next()._1

      val systemColId = schema.filterNot {
        (colId, colInfo) => colInfo.typ != SoQLID
      }.iterator.next()._1

      (systemColId, versionColId)
    }

  
    test("Universe can create a table") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val blankTableSchema = getSchema(pgu, copyInfo)
        assert(blankTableSchema.size == 0, "We expect no columns")
      }
    }

    def dummyValues():Map[TypeName, SoQLValue] = {
      SoQLType.typesByName map {
        case ((name, typ)) => {
          val dummyVal = typ match {
            case SoQLText => SoQLText("Hello World")
            case SoQLID => SoQLID(0)
            case SoQLVersion => SoQLVersion(0)
            case SoQLBoolean => SoQLBoolean.canonicalTrue
            case SoQLNumber => SoQLNumber(new BigDecimal(0.0))
            case SoQLMoney  => SoQLMoney(new BigDecimal(0.0))
            case SoQLDouble => SoQLDouble(0.1)
            case SoQLFixedTimestamp => SoQLFixedTimestamp(new DateTime())
            case SoQLFloatingTimestamp => SoQLFloatingTimestamp(new LocalDateTime())
            case SoQLDate => SoQLDate(new LocalDate())
            case SoQLTime => SoQLTime(new LocalTime())
            case SoQLObject => SoQLObject(new JObject(Map("hi" -> JString("there"))))
            case SoQLArray => SoQLArray(new JArray(Seq(JString("there"))))
            case SoQLJson => SoQLJson(new JArray(Seq(JString("there"))))
            case SoQLLocation => SoQLLocation(0.1, 0.2)
            case SoQLNull => SoQLNull
          }
          (name, dummyVal)
        }
      }
    }

    test("Universe can add columns") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)

        val cols = SoQLType.typesByName filterKeys (!Set(TypeName("json")).contains(_)) map {
          case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
        }
        sLoader.addColumns(cols)

        // if you want to examine the tables...
        //pgu.commit
        validateSchema(cols, getSchema(pgu, copyInfo))
      }
    }

    test("Universe can del columns") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val types = SoQLType.typesByName filterKeys (!Set(TypeName("json")).contains(_))

        val cols = types map {
          case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
        }
        sLoader.addColumns(cols)

        validateSchema(cols, getSchema(pgu, copyInfo))
        // +1 since location maps to lat and lon columns, so this test is pretty fragile in the face of new data types
        assert(jdbcColumnCount(conn, copyInfo.dataTableName) == cols.size+1, s"Expected table to have ${cols.size+1} columns")

        cols.foreach(pgu.datasetMapWriter.dropColumn(_))

        try {
          sLoader.dropColumns(cols)
        } catch {
          case pex:PSQLException => println("Failing Query: " + pex.getServerErrorMessage.getHint + " - " + pex.getSQLState); throw pex
        }

        assert(getSchema(pgu, copyInfo).size == 0, "We expect no columns");
        assert(jdbcColumnCount(conn, copyInfo.dataTableName) == 0, s"Expected table to have no columns")
      }
    }

    test("Universe can insert rows") {
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
        dummyVals filterKeys (!Set(TypeName("json")).contains(_)) foreach {
          (v) => assert(rowValues.contains(v._2), "Could not find " + v + " in row values: " + rowValues)
        }
      }

    }

    test("Universe can update rows") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val schema = createTableWithSchema(pgu, copyInfo, sLoader)

        // Get the column id of the SoQLText object
        val textColId = schema.filterNot {
          (colId, colInfo) => colInfo.typ != SoQLText
        }.iterator.next()._1

        val (systemColId, versionColId) = getSpecialColumnIds(schema)

        // Setup our row data for insert with a "notupdated" field
        val dummyVals = dummyValues()
        for (id <- 1 until 10) {
          insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)).updated(SoQLText.name, SoQLText("notupdated")), pgu, copyInfo, schema)
        }

        val expect = SoQLText("updated")

        // Perform the update
        val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
        val loader = pgu.prevettedLoader(copyCtx, pgu.logger(copyInfo.datasetInfo, "test-user"))


        for (id <- 1 until 10) {
          val rowId = new RowId(id)
          // Setup our row; adding in the special version and system column values
          val colIdMap = schema.foldLeft(ColumnIdMap[SoQLValue]())  { (acc, kv) =>
            val (cId, columnInfo) = kv
            acc + (cId -> dummyVals.get(columnInfo.typ.name).get)
          }
          val newRow =  colIdMap + (textColId -> expect) + (versionColId -> SoQLVersion(2)) + (systemColId -> SoQLID(id))

          loader.update(rowId, None, newRow)
          loader.flush()
        }

        // Verify every row was updated
        for (id <- 1 until 10) {
          val rowId = new RowId(id)
          val result = getRow(rowId, pgu, copyInfo, schema)
          assert(result.size == 1, "Expected only a single row for id " + id + " but got " + result.size)
          assert(result.get(SoQLID(id)).get.row.get(textColId).get == expect)
        }
      }
    }

    test("Universe can delete rows") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val schema = createTableWithSchema(pgu, copyInfo, sLoader)

        // Setup our row data for insert with a "notupdated" field
        val dummyVals = dummyValues()
        for (id <- 1 until 10) {
          insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)).updated(SoQLText.name, SoQLText("notupdated")), pgu, copyInfo, schema)
        }
        val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
        val loader = pgu.prevettedLoader(copyCtx, pgu.logger(copyInfo.datasetInfo, "test-user"))
        for (id <- 1 until 10) {
          val rowId = new RowId(id)
          loader.delete(rowId, None)
        }
        loader.flush()
        val result = getRow(new RowId(5), pgu, copyInfo, schema)
        assert(result.size == 0, "Should not have rows!")
      }
    }

    test("Universe can read a row by id") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val schema = createTableWithSchema(pgu, copyInfo, sLoader)

        // Setup our row data
        val dummyVals = dummyValues()

        for (id <- 1 until 10) {
          insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)), pgu, copyInfo, schema)
        }

        for (id <- 1 until 10) {
          val result = getRow(new RowId(id), pgu, copyInfo, schema)
          assert(result.size == 1, "Expected only a single row for id " + id + " but got " + result.size)
        }
      }
    }

    test("Universe can delete table") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn)
        sLoader.drop(copyInfo)
        // Can't delete the table if it's the only instance of the table that exists
        // Can't delete the table if it's published
        // Maybe can delete the table if it's a snapshot?
        //        pgu.datasetMapWriter
        //        pgu.commit
        println(copyInfo.dataTableName)
        // Check to see if dropped table is in pending drop table operations collection
      }
    }

    test("Universe can delete a copy of a dataset") {

    }

    test("Universe can delete a published copy") {

    }

}
