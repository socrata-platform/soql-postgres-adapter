package com.socrata.pg.store

import java.io.{File, FileInputStream, InputStreamReader}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.io.Codec

import com.rojoma.json.v3.ast.{JArray, JValue}
import com.rojoma.json.v3.io.JsonEventIterator
import com.rojoma.json.v3.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.simplearm.util._
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
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Recreate test databases of truth and secondary.
 * Provide the tools for creating a dataset and replicated to secondary.
 */
trait DatabaseTestBase extends Logging {

  val dcInstance: String // alpha

  val project: String // soql-server-pg

  val projectDb: String // Each project has its own databases

  val storeId: String // pg

  private lazy val projectConfig: Config = ConfigFactory.load().getConfig(s"com.socrata.pg.${projectDb}")

  lazy val config: Config = projectConfig.getConfig("secondary")

  lazy val truthDataSourceConfig: DataSourceConfig = new DataSourceConfig(projectConfig, "truth.database" )

  lazy val truthDb: String = truthDataSourceConfig.database

  lazy val secondaryDb: String = config.getString("database.database")

  var truthDatasetId: DatasetId = _

  var secDatasetId: DatasetId = _

  val idMap =  (cn: ColumnName) => new UserColumnId(cn.name)

  val datasetIdToInternal = (datasetId: DatasetId) => s"${dcInstance}.${datasetId.underlying}"

  /**
   * Create both truth and secondary databases
   * To be called during beforeAll
   */
  def createDatabases(): Unit = {
    DatabaseTestBase.createDatabases(truthDb, secondaryDb, config)
  }

  def withSoQLCommon[T](datasourceConfig: DataSourceConfig)(f: (SoQLCommon) => T): T = {
    val result = DataSourceFromConfig(truthDataSourceConfig).map { dsc =>
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
    result
  }

  /**
   * @param conn
   * @return truth dataset id and secondary dataset id pair.
   */
  def importDataset(conn: Connection): Tuple2[DatasetId, DatasetId] = {
    withSoQLCommon(truthDataSourceConfig) { common =>
      val ProcessCreationReturns(datasetId, _, _, _) = processMutationCreate(common, fixtureFile("mutate-create.json"))
      processMutation(common, fixtureFile("mutate-publish.json"), datasetId)
      pushToSecondary(common, datasetId)
      truthDatasetId = datasetId
      val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon)
      val dsName = s"$dcInstance.${datasetId.underlying}"
      secDatasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(s"$dsName").getOrElse(
        throw new Exception("Fail to get secondary database id"))
      pgu.datasetMapReader.datasetInfo(secDatasetId).getOrElse(throw new Exception("fail to get dataset from secondary"))
      (truthDatasetId, secDatasetId)
    }
  }

  def createRollup(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo): String = {
    val rollupInfo = pgu.datasetMapWriter.createOrUpdateRollup(copyInfo, new RollupName("roll1"), "select 'x'")
    val rm = new RollupManager(pgu, copyInfo)
    rm.updateRollup(rollupInfo, copyInfo.dataVersion)
    val rollupTableName = RollupManager.rollupTableName(rollupInfo, copyInfo.dataVersion)
    pgu.commit()
    rollupTableName
  }

  def fixtureRows(name: String): Iterator[JValue] = {
    val file = fixtureFile(name)
    val inputStream = new FileInputStream(file)
    val jsonEventIter = new JsonEventIterator(new InputStreamReader(inputStream, Codec.UTF8.charSet))
    JsonArrayIterator.fromEvents[JValue](jsonEventIter)
  }

  def processMutationCreate(common: SoQLCommon, script: File): ProcessCreationReturns = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } yield {
      val array = JsonUtil.readJsonFile[JArray](script, Codec.UTF8).right.toOption.get
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.createScript(u, array.iterator)
    }
  }

  def processMutation(common: SoQLCommon, script: File, datasetId: DatasetId): ProcessMutationReturns = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } yield {
      val array = JsonUtil.readJsonFile[JArray](script, Codec.UTF8).right.toOption.get
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.updateScript(u, datasetId, array.iterator)
    }
  }

  def pushToSecondary(common: SoQLCommon, datasetId: DatasetId, isNew: Boolean = true): Unit = {
    common.universe.map { u =>
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

  def fixtureFile(name: String): File = {
    val rootDir = System.getProperty("user.dir")
    new File(rootDir + s"/$project/src/test/resources/fixtures/" + name)
  }

  private def resultSetHasRows(conn: Connection, sql: String): Boolean = {
    using (conn.prepareStatement(sql)) { stmt: PreparedStatement =>
      using (stmt.executeQuery()) { (rs: ResultSet) =>
        rs.next()
      }
    }
  }
}

object DatabaseTestBase extends Logging {

  private var dbInitialized = false

  def createDatabases(truthDb: String, secondaryDb: String, secondaryConfig: Config): Unit = {
    synchronized {
      if (!DatabaseTestBase.dbInitialized) {
        logger.info("*** Creating test databases *** ")
        createDatabase(truthDb)
        createDatabase(secondaryDb)
        // migrate truth db
        populateTruth(truthDb)
        // migrate secondary db
        SchemaMigrator("database", MigrationOperation.Migrate, secondaryConfig)
        DatabaseTestBase.dbInitialized = true
      }
    }
  }

  private def createDatabase(dbName: String): Unit = {
    try {
      Class.forName("org.postgresql.Driver").newInstance()
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
