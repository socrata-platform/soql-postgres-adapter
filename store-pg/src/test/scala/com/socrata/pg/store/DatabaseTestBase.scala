package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.rojoma.json.util.{JsonArrayIterator, JsonUtil}
import com.rojoma.json.ast.{JValue, JArray}
import com.rojoma.json.io.JsonEventIterator
import com.socrata.datacoordinator.id.{UserColumnId, DatasetId}
import com.socrata.datacoordinator.common.SoQLCommon
import com.socrata.datacoordinator.util.{IndexedTempFile, NullCache, NoopTimingReport}
import com.socrata.datacoordinator.service.Mutator
import com.socrata.datacoordinator.secondary.NamedSecondary
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation
import com.socrata.datacoordinator.truth.sql.{DatabasePopulator => TruthDatabasePopulator, DatasetMapLimits}
import com.socrata.datacoordinator.truth.universe.sql.PostgresCopyIn
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.soql.environment.ColumnName
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.Logging
import java.io.{InputStreamReader, FileInputStream, File}
import java.sql.{DriverManager, Connection}
import java.util.concurrent.TimeUnit
import java.util.UUID
import org.postgresql.ds.PGSimpleDataSource
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.io.Codec


/**
 * Recreate test databases of truth and secondary.
 * Provide the tools for creating a dataset and replicated to secondary.
 */
trait DatabaseTestBase extends Logging {  //this: Matchers =>

  val dcInstance: String // alpha

  val project: String // soql-server-pg

  val storeId: String // pg

  val truthDb = "truth_test"

  val secondaryDb = "secondary_test"

  val config: Config = ConfigFactory.load().getConfig("com.socrata.pg.common")

  var truthDatasetId: DatasetId = _

  var secDatasetId: DatasetId = _

  val idMap =  (cn: ColumnName) => new UserColumnId(cn.name)

  val datasetIdToInternal = (datasetId: DatasetId) => s"${dcInstance}.${datasetId.underlying}"

  /**
   * Create both truth and secondary databases
   * To be called during beforeAll
   */
  def createDatabases() {
    synchronized {
      if (!DatabaseTestBase.dbInitialized) {
        DatabaseTestBase.dbInitialized = true
        logger.info("*** Creating test databases ***")
        createDatabase(truthDb)
        createDatabase(secondaryDb)
        // migrate truth db
        populateTruth(truthDb)
        // migrate secondary db
        SchemaMigrator("database", MigrationOperation.Migrate, PGSecondaryUtil.config)
      }
    }
  }

  def importDataset(conn: Connection, dbName: String = truthDb) {
    val ds = new PGSimpleDataSource
    ds.setServerName("localhost")
    ds.setPortNumber(5432)
    ds.setUser("blist")
    ds.setPassword("blist")
    ds.setDatabaseName(dbName)

    val executor = java.util.concurrent.Executors.newCachedThreadPool()

    val common = new SoQLCommon(
      ds,
      PostgresCopyIn,
      executor,
      Function.const(None),
      NoopTimingReport,
      true,
      Duration(10, TimeUnit.SECONDS),
      dcInstance,
      new File(System.getProperty("java.io.tmpdir")),
      NullCache
    )

    val (datasetId, _) = processMutationCreate(common, fixtureFile("mutate-create.json"))
    processMutation(common, fixtureFile("mutate-publish.json"), datasetId)
    pushToSecondary(common, datasetId)

    truthDatasetId = datasetId
    val pgu = new PGSecondaryUniverse[SoQLType, SoQLValue](conn,  PostgresUniverseCommon )
    val dsName = s"$dcInstance.${truthDatasetId.underlying}"
    secDatasetId = pgu.secondaryDatasetMapReader.datasetIdForInternalName(s"$dsName").getOrElse(
      throw new Exception("Fail to get secondary database id"))
  }

  def fixtureRows(name: String): Iterator[JValue] = {
    val file = fixtureFile(name)
    val inputStream = new FileInputStream(file)
    val jsonEventIter = new JsonEventIterator(new InputStreamReader(inputStream, Codec.UTF8.charSet))
    JsonArrayIterator[JValue](jsonEventIter)
  }

  private def processMutationCreate(common: SoQLCommon, script: File) = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } yield {
      val array = JsonUtil.readJsonFile[JArray](script, Codec.UTF8).get
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.createScript(u, array.iterator)
    }
  }

  private def processMutation(common: SoQLCommon, script: File, datasetId: DatasetId) = {
    for {
      u <- common.universe
      indexedTempFile <- managed(new IndexedTempFile(10 * 1024, 10 * 1024))
    } yield {
      val array = JsonUtil.readJsonFile[JArray](script, Codec.UTF8).get
      val mutator = new Mutator(indexedTempFile, common.Mutator)
      mutator.updateScript(u, datasetId, array.iterator)
    }
  }

  private def pushToSecondary(common: SoQLCommon, datasetId: DatasetId) {
    for(u <- common.universe) {
      val secondary = new PGSecondary(config)
      val pb = u.playbackToSecondary
      val secondaryMfst = u.secondaryManifest
      val claimantId = UUID.randomUUID()
      val claimTimeout = FiniteDuration(1, TimeUnit.MINUTES)
      secondaryMfst.addDataset(storeId, datasetId)
      for(job <- secondaryMfst.claimDatasetNeedingReplication(storeId, claimantId, claimTimeout)) {
        try {
          pb(NamedSecondary(storeId, secondary), job)
        } finally {
          secondaryMfst.releaseClaimedDataset(job)
        }
      }
    }
  }

  private def createDatabase(dbName: String) {
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
  }

  private def populateTruth(dbName: String) {
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      com.socrata.datacoordinator.truth.migration.Migration.migrateDb(conn)
    }
  }

  private def fixtureFile(name: String): File = {
    val rootDir = System.getProperty("user.dir")
    new File(rootDir + s"/$project/src/test/resources/fixtures/" + name)
  }
}

object DatabaseTestBase {

  private var dbInitialized = false

}