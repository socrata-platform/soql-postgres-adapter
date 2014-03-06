package com.socrata.pg.store

import org.apache.log4j.PropertyConfigurator
import org.joda.time.{LocalTime, LocalDate, LocalDateTime, DateTime}
import com.rojoma.json.ast.{JArray, JString, JObject}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{RowId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator => TruthDatabasePopulator, DatasetMapLimits}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types._
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.ConfigFactory
import java.sql.{DriverManager, Connection}
import org.scalatest.{BeforeAndAfterAll, Matchers}

trait PGSecondaryUniverseTestBase {
  this : Matchers with BeforeAndAfterAll =>

  type CT = SoQLType
  type CV = SoQLValue
  val common = PostgresUniverseCommon

  override def beforeAll() {
    val config = ConfigFactory.load().getConfig("com.socrata.pg.common")
    PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))
  }

  override def afterAll() {
  }

  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/secondary_test?loglevel=$loglevel", "blist", "blist")) { conn =>
      conn.setAutoCommit(false)
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
        existing should contain (colInfo.systemId, colInfo.typ)
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

  def fetchColumnFromTable(connection: Connection, options: Map[String, String]) = {
    val tableName = options.getOrElse("tableName", "")
    val columnName = options.getOrElse("columnName", "")
    val whereClause = options.getOrElse("whereClause", "")
    val statement = connection.prepareStatement(s"SELECT $columnName FROM $tableName WHERE $whereClause")
    using(statement.executeQuery) { rs =>
      if (rs.next) {
        rs.getString(columnName)
      } else {
        ""
      }
    }
  }

  def updateColumnValueInTable(connection: Connection, options: Map[String, String]) {
    val tableName = options.getOrElse("tableName", "")
    val columnName = options.getOrElse("columnName", "")
    val columnValue = options.getOrElse("columnValue", "")
    val whereClause = options.getOrElse("whereClause", "")
    val statement = connection.prepareStatement(s"UPDATE $tableName SET $columnName = $columnValue WHERE $whereClause")
    statement.execute
  }

  def dummyValues():Map[TypeName, SoQLValue] = {
    SoQLType.typesByName map {
      case ((name, typ)) => {
        val dummyVal = typ match {
          case SoQLText => SoQLText("Hello World")
          case SoQLID => SoQLID(0)
          case SoQLVersion => SoQLVersion(0)
          case SoQLBoolean => SoQLBoolean.canonicalTrue
          case SoQLNumber => SoQLNumber(new java.math.BigDecimal(0.0))
          case SoQLMoney  => SoQLMoney(new java.math.BigDecimal(0.0))
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
}
