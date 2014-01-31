package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import org.scalatest.matchers.MustMatchers

import org.scalatest.prop.PropertyChecks
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.soql.types.{SoQLText, SoQLValue, SoQLType}
import com.socrata.datacoordinator.id.{UserColumnId, CopyId, DatasetId}
import com.socrata.datacoordinator.truth.metadata.{CopyPair, CopyInfo, LifecycleStage, DatasetInfo}
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.datacoordinator.common.{StandardDatasetMapLimits, DataSourceConfig, DataSourceFromConfig, DatabaseCreator}
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, DatabasePopulator}
import com.socrata.soql.environment.TypeName


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

    test("Universe can create a table") {
      withDb() { conn =>
        val pgu = new PGSecondaryUniverse[CT, CV](conn,  PostgresUniverseCommon )
        val copyInfo = pgu.datasetMapWriter.create("us") // locale
        val sLoader = pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue])
        sLoader.create(copyInfo)

        val cols = SoQLType.typesByName filterKeys (!Set(TypeName("json")).contains(_)) map {
          case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
        }
        sLoader.addColumns(cols)

        // if you want to examine the tables...
        //pgu.commit

        // Did we do it?
        val existing = for (reader <- pgu.datasetReader.openDataset(copyInfo)) yield {
          reader.schema.values map {
              colInfo => (colInfo.systemId, colInfo.typeName)
          }
        }.toMap

        cols foreach {
          colInfo =>  {
            existing must contain (colInfo.systemId, colInfo.typeName)
            println("Checked that " + colInfo.userColumnId + ":" + colInfo.typeName + " was truely and surely created")
          }
        }
      }
    }

}
