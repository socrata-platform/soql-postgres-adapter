package com.socrata.pg.store

import java.sql.{Connection, DriverManager}
import java.util.UUID

import com.rojoma.json.v3.ast.{JArray, JObject, JString}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{RowId, UserColumnId}
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetCopyContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types._
import com.typesafe.config.Config
import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

trait PGSecondaryUniverseTestBase extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  type CT = SoQLType
  type CV = SoQLValue
  val common = PostgresUniverseCommon

  val config: Config

  def withDb[T]()(f: (Connection) => T): T = {
    def loglevel = 0; // 2 = debug, 0 = default

    val database = config.getString("database.database")
    val user = config.getString("database.username")
    val pass = config.getString("database.password")
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$database?loglevel=$loglevel", user, pass)) { conn =>
      conn.setAutoCommit(false)
      f(conn)
    }
  }

  def withPgu[T]()(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb() { conn =>
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn, PostgresUniverseCommon)
      f(pgu)
    }
  }

  def createTable(conn:Connection, datasetInfo:Option[DatasetInfo] = None): (PGSecondaryUniverse[SoQLType, SoQLValue], CopyInfo, SchemaLoader[SoQLType]) = {
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon, datasetInfo)
    val copyInfo = pgu.datasetMapWriter.create("us") // locale
    val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
    sLoader.create(copyInfo)
    (pgu, copyInfo, sLoader)
  }

  def createTableWithSchema(pgu:PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo:CopyInfo, sLoader:SchemaLoader[SoQLType]) = {
    // Setup the data columns
    val cols = SoQLType.typesByName filterKeys (!UnsupportedTypes.contains(_)) map {
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
        existing should contain ((colInfo.systemId, colInfo.typ))
      }
    }
  }

  def jdbcColumnCount(conn:Connection, tableName:String): Integer = {
    using(conn.getMetaData.getColumns(null, null, tableName, null)) { rs =>
      rs.last()
      rs.getRow
    }
  }

  def jdbcRowCount(conn: Connection, tableName: String): Integer = {
    using(conn.prepareStatement(s"SELECT COUNT(*) FROM ${tableName}")) { stmt =>
      using(stmt.executeQuery()) { rs =>
        rs.next()
        rs.getInt(1)
      }
    }
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
    SoQLType.typesByName.filterKeys(!UnsupportedTypes.contains(_)) map {
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
          case SoQLPoint => SoQLPoint(SoQLPoint.WktRep.unapply("POINT (47.6303 -122.3148)").get)
          case SoQLMultiPoint => SoQLMultiPoint(SoQLMultiPoint.WktRep.unapply("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))").get)
          case SoQLLine => SoQLLine(SoQLLine.WktRep.unapply("LINESTRING (30 10, 10 30, 40 40)").get)
          case SoQLMultiLine => SoQLMultiLine(SoQLMultiLine.WktRep.unapply("MULTILINESTRING ((100 0, 101 1), (102 2, 103 3))").get)
          case SoQLPolygon => SoQLPolygon(SoQLPolygon.WktRep.unapply("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))").get)
          case SoQLMultiPolygon => SoQLMultiPolygon(SoQLMultiPolygon.WktRep.unapply(
            "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))").get)
          case SoQLBlob => SoQLBlob(UUID.randomUUID().toString)
          case SoQLNull => SoQLNull
        }
        (name, dummyVal)
      }
    }
  }

  /**
   * TODO: Remove types in this set once support is added.
   */
  val UnsupportedTypes = Set("json").map(TypeName(_))
}
