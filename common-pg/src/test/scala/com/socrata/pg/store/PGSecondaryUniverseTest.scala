package com.socrata.pg.store

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.MustMatchers
import org.scalatest.prop.PropertyChecks
import java.sql.{DriverManager, Connection}
import com.rojoma.simplearm.util._
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.id.{CopyId, DatasetId}
import com.socrata.datacoordinator.truth.metadata.{CopyPair, CopyInfo, LifecycleStage, DatasetInfo}
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.datacoordinator.common.{StandardDatasetMapLimits, DataSourceConfig, DataSourceFromConfig, DatabaseCreator}
import com.socrata.datacoordinator.truth.sql.{DatasetMapLimits, DatabasePopulator}


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
      using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/secondary_test", "blist", "blist")) { conn =>
        conn.setAutoCommit(false)
        populateDatabase(conn)
        f(conn)
      }
    }

    test("Universe can create a table") {
      withDb() { conn =>
        val pgu = new PGSecondaryUniverse[CT, CV](conn,  PostgresUniverseCommon )
        val copyInfo = pgu.datasetMapWriter.create("us")
        pgu.schemaLoader(new PGSecondaryLogger[SoQLType, SoQLValue]).create(copyInfo)
      }
    }


}
