package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import org.scalatest.matchers.MustMatchers

import org.scalatest.prop.PropertyChecks
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.soql.types.{SoQLValue, SoQLText, SoQLType}
import com.socrata.datacoordinator.id.{UserColumnId, CopyId, DatasetId}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.datacoordinator.common.{StandardDatasetMapLimits, DataSourceConfig, DataSourceFromConfig, DatabaseCreator}
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, DatabasePopulator}
import com.socrata.soql.environment.TypeName
import com.socrata.datacoordinator.truth.loader.SchemaLoader
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
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
      val sql = DatabasePopulator.metadataTablesCreate(DatasetMapLimits())
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
          println("Checked that " + colInfo.userColumnId+ ":" + colInfo.typeName + " was truly and surely created")
        }
      }
    }

    test("Universe can create a table") {
      withDb() { conn =>
        val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
        val blankTableSchema = getSchema(pgu, copyInfo)
        assert(blankTableSchema.size == 0, "We expect no columns");
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

    }

    test("Universe can insert rows") {

    }

    test("Universe can update rows") {

    }

    test("Universe can delete rows") {

    }

    test("Universe can delete table") {

    }

}
