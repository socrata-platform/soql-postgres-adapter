package com.socrata.pg.store

import java.io.{File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.io.Codec
import com.rojoma.json.v3.ast.{JArray, JValue, JObject, JString}
import com.rojoma.json.v3.io.{JsonEventIterator, JsonReader}
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig, SoQLCommon}
import com.socrata.datacoordinator.id.{DatasetId, RollupName, UserColumnId}
import com.socrata.datacoordinator.secondary.NamedSecondary
import com.socrata.datacoordinator.service.{Mutator, ProcessCreationReturns, ProcessMutationReturns}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo}
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.datacoordinator.util.{IndexedTempFile, NoopTimingReport, NullCache}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.scalatest.Matchers.fail
import com.socrata.pg.config.StoreConfig

/**
 * Recreate test databases of truth and secondary.
 * Provide the tools for creating a dataset and replicated to secondary.
 */
trait DatabaseTestBase extends TestResourceNames {

  val dcInstance: String // alpha

  val projectClassLoader: ClassLoader

  val projectDb: String // Each project has its own databases

  val storeId: String // pg

  private lazy val projectConfig: Config = ConfigFactory.load().getConfig(s"com.socrata.pg.${projectDb}")

  lazy val secondaryConfig: Config = projectConfig.getConfig("secondary")

  lazy val config = new StoreConfig(secondaryConfig, "")

  lazy val truthDataSourceConfig: DataSourceConfig = new DataSourceConfig(projectConfig, "truth.database" )

  lazy val truthDb: String = truthDataSourceConfig.database

  lazy val secondaryDb: String = secondaryConfig.getString("database.database")

  var truthDatasetId: DatasetId = _

  var secDatasetId: DatasetId = _

  val idMap =  (cn: ColumnName) => new UserColumnId(cn.caseFolded)

  val datasetIdToInternal = (datasetId: DatasetId) => s"${dcInstance}.${datasetId.underlying}"

  /**
   * Create both truth and secondary databases
   * To be called during beforeAll
   */
  def createDatabases(): Unit = {
    DatabaseTestBase.createDatabases(truthDb, secondaryDb, config)
  }

  def withSoQLCommon[T](datasourceConfig: DataSourceConfig)(f: (SoQLCommon) => T): T = {
    DataSourceFromConfig(truthDataSourceConfig).run { dsc =>
      val executor = java.util.concurrent.Executors.newCachedThreadPool()
      try {
        val common = new SoQLCommon(
          dsc.dataSource,
          PostgresCopyIn,
          executor,
          Function.const(None),
          NoopTimingReport,
          true,
          Duration(10, TimeUnit.SECONDS),
          dcInstance,
          new File(System.getProperty("java.io.tmpdir")),
          Duration(10, TimeUnit.SECONDS),
          Duration(10, TimeUnit.SECONDS),
          NullCache
        )
        f(common)
      } finally {
        executor.shutdown()
      }
    }
  }

  /**
   * @param conn
   * @return truth dataset id and secondary dataset id pair.
   */
  def importDataset(conn: Connection, mutationScript: String = "mutate-create.json", forceUnique: Option[String] = None): Tuple2[DatasetId, DatasetId] = {
    withSoQLCommon(truthDataSourceConfig) { common =>
      val ProcessCreationReturns(datasetId, _, _, _, _) = processMutationCreate(common, fixtureFile[JArray](mutationScript), forceUnique)
      processMutation(common, fixtureFile[JArray]("mutate-publish.json"), datasetId)
      pushToSecondary(common, datasetId)
      truthDatasetId = datasetId
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon)
      val dsName = s"$dcInstance.${datasetId.underlying}"
      secDatasetId = pgu.datasetMapReader.datasetIdForInternalName(s"$dsName").getOrElse(
        throw new Exception("Fail to get secondary database id"))
      pgu.datasetMapReader.datasetInfo(secDatasetId).getOrElse(throw new Exception("fail to get dataset from secondary"))
      (truthDatasetId, secDatasetId)
    }
  }

  def createRollup(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo): String = {
    val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollupSecondary(copyInfo, new RollupName("roll1"), "select 'x'", None).getOrElse(fail("Could not create rollup"))
    val rm = new RollupManager(pgu, copyInfo)
    rm.updateRollup(rollupInfo, None, Function.const(false))
    pgu.commit()
    rollupInfo.tableName
  }

  def fixtureRows(name: String): Iterator[JValue] = {
    fixtureFile[JArray](name).iterator
  }

  class UniquifyResourceName(underlying: Iterator[JValue], forceUnique: Option[String] = None) extends Iterator[JValue] {
    private var first = true

    override def hasNext = underlying.hasNext
    override def next() = {
      val item = underlying.next()
      if(first) {
        first = false
        val JObject(obj) = item
        val rn = obj.get("resource") match {
          case Some(JString(name)) =>
            forceUnique match {
              case Some(u) => s"${name}_${u}"
              case None => name
            }
          case Some(_) => throw new Exception("resource was not a string")
          case None => freshResourceNameRaw()
        }
        JObject(obj.toMap + ("resource" -> JString(rn)))
      } else {
        item
      }
    }
  }

  def processMutationCreate(common: SoQLCommon, script: JArray, forceUnique: Option[String] = None): ProcessCreationReturns = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } {
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.createScript(u, new UniquifyResourceName(script.iterator, forceUnique))
    }
  }

  def processMutation(common: SoQLCommon, script: JArray, datasetId: DatasetId): ProcessMutationReturns = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } {
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.updateScript(u, datasetId, script.iterator)
    }
  }

  def pushToSecondary(common: SoQLCommon, datasetId: DatasetId, isNew: Boolean = true): Unit = {
    common.universe.run { u =>
      val secondary = new PGSecondary(config)
      val pb = u.playbackToSecondary
      val secondaryMfst = u.secondaryManifest
      val claimantId = UUID.randomUUID()
      val claimTimeout = FiniteDuration(1, TimeUnit.MINUTES)
      if (isNew) secondaryMfst.addDataset(storeId, datasetId)
      secondaryMfst.claimDatasetNeedingReplication(storeId, claimantId, claimTimeout).foreach { job =>
        try {
          pb(NamedSecondary(storeId, secondary, ""), job)
        } finally {
          secondaryMfst.releaseClaimedDataset(job)
        }
      }
    }
  }

  /**
   * Check if there are base table, audit table or log table.
   */
  def hasDataTables(conn: Connection, tableName: String, datasetInfo: DatasetInfo): Boolean = {
    val sql = s"""
      SELECT * FROM pg_class
       WHERE relname in ('$tableName', '${datasetInfo.auditTableName}', '${datasetInfo.logTableName}')
         And relkind='r'
      """
    resultSetHasRows(conn, sql)
  }

  /**
   * Check if there are rollup tables.
   * @param conn
   */
  def hasRollupTables(conn: Connection, tableName: String): Boolean = {
    val sql = s"SELECT * FROM pg_class WHERE relname like '${tableName}_r%' and relkind='r'"
    resultSetHasRows(conn, sql)
  }

  def fixtureFile[T : JsonDecode](name: String): T = {
    using(projectClassLoader.getResourceAsStream("fixtures/" + name)) { stream =>
      if(stream == null) throw new Exception("Unable to find fixture " + name)
      JsonUtil.readJson[T](new InputStreamReader(stream, StandardCharsets.UTF_8)) match {
        case Right(res) => res
        case Left(err) => throw new Exception(s"Error processing fixture $name: ${err.english}")
      }
    }
  }

  private def resultSetHasRows(conn: Connection, sql: String): Boolean = {
    using (conn.prepareStatement(sql)) { stmt: PreparedStatement =>
      using (stmt.executeQuery()) { (rs: ResultSet) =>
        rs.next()
      }
    }
  }
}

object DatabaseTestBase {
  val logger = Logger[DatabaseTestBase]

  private var dbInitialized = false

  def createDatabases(truthDb: String, secondaryDb: String, secondaryConfig: StoreConfig): Unit = {
    synchronized {
      if (!DatabaseTestBase.dbInitialized) {
        logger.info("*** Creating test databases *** ")
        createDatabase(truthDb)
        createDatabase(secondaryDb)
        // migrate truth db
        populateTruth(truthDb)
        // migrate secondary db
        SchemaMigrator(MigrationOperation.Migrate, secondaryConfig.database , false)
        DatabaseTestBase.dbInitialized = true
      }
    }
  }

  private def createDatabase(dbName: String): Unit = {
    try {
      Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance()
    } catch {
      case ex: ClassNotFoundException => throw ex
    }
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      val sql = s"drop database if exists $dbName; create database $dbName;"
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      val sql = "create extension postgis;"
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }
  }

  private def populateTruth(dbName: String): Unit = {
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      com.socrata.datacoordinator.truth.migration.Migration.migrateDb(conn)
    }
  }
}
