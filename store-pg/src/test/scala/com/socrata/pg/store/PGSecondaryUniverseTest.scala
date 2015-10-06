package com.socrata.pg.store

import java.sql.Connection

import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.secondary
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.truth.universe.sql.SqlTableCleanup
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.TypeName
import com.socrata.soql.types._
import org.postgresql.util.PSQLException
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

/**
 *
 */
class PGSecondaryUniverseTest extends FunSuite with Matchers with BeforeAndAfterAll
      with PGSecondaryUniverseTestBase with PGStoreTestBase with DatabaseTestBase  {

  override def beforeAll(): Unit = {
    createDatabases()
  }

  test("Universe can create a table") {
    withDb() { conn =>
      val (pgu, copyInfo, _) = createTable(conn:Connection)
      val blankTableSchema = getSchema(pgu, copyInfo)
      assert(blankTableSchema.size == 0, "We expect no columns")
    }
  }

  test("Universe creates table with passed-in obfuscation key") {
    val expected = "super secret".getBytes()
    val dsInfo = new secondary.DatasetInfo("not-used", "not-used", expected)
    withDb() { conn =>
      val (pgu, copyInfo, _) = createTable(conn:Connection, Some(dsInfo))
      assert(copyInfo.datasetInfo.obfuscationKey == expected, "Expected that the dataset used our obfuscation key")
    }
  }

  test("Universe can add columns") {
    withDb() { conn =>
      val (pgu, copyInfo, sLoader) = createTable(conn:Connection)

      val cols = SoQLType.typesByName filterKeys (!UnsupportedTypes.contains(_)) map {
        case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
      }
      sLoader.addColumns(cols)

      // if you want to examine the tables...
      // pgu.commit
      validateSchema(cols, getSchema(pgu, copyInfo))
    }
  }

  test("Universe can del columns") {
    withDb() { conn =>
      val (pgu, copyInfo, sLoader) = createTable(conn:Connection)
      val types = SoQLType.typesByName filterKeys (!UnsupportedTypes.contains(_))

      val cols = types map {
        case (n, t) => pgu.datasetMapWriter.addColumn(copyInfo, new UserColumnId(n + "_USERNAME"), t, n + "_PHYSNAME")
      }
      sLoader.addColumns(cols)

      validateSchema(cols, getSchema(pgu, copyInfo))
      assert(jdbcColumnCount(conn, copyInfo.dataTableName) == cols.size, s"Expected table to have ${cols.size} columns")

      cols.foreach(pgu.datasetMapWriter.dropColumn(_))

      try {
        sLoader.dropColumns(cols)
      } catch {
        case pex:PSQLException =>
          throw new TestFailedException(
            "Failing Query: " + pex.getServerErrorMessage.getHint + " - " + pex.getSQLState,
            pex,
            5)
      }

      assert(getSchema(pgu, copyInfo).size == 0, "We expect no columns")
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

      // Check that all our dummy values can be read; except for unsupported types.
      dummyVals filterKeys (!UnsupportedTypes.contains(_)) foreach {
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
      (1 until 10).foreach { id =>
        insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)).updated(SoQLText.name, SoQLText("notupdated")), pgu, copyInfo, schema)
      }

      val expect = SoQLText("updated")

      // Perform the update
      val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
      val loader = pgu.prevettedLoader(copyCtx, pgu.logger(copyInfo.datasetInfo, "test-user"))


      (1 until 10).foreach { id =>
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
      (1 until 10).foreach { id =>
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
      (1 until 10).foreach { id =>
        insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)).updated(SoQLText.name, SoQLText("notupdated")), pgu, copyInfo, schema)
      }
      val copyCtx = new DatasetCopyContext[SoQLType](copyInfo, schema)
      val loader = pgu.prevettedLoader(copyCtx, pgu.logger(copyInfo.datasetInfo, "test-user"))
      (1 until 10).foreach { id =>
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

      (1 until 10).foreach { id =>
        insertDummyRow(new RowId(id), dummyVals.updated(SoQLID.name,SoQLID(id)), pgu, copyInfo, schema)
      }

      (1 until 10).foreach { id =>
        val result = getRow(new RowId(id), pgu, copyInfo, schema)
        assert(result.size == 1, "Expected only a single row for id " + id + " but got " + result.size)
      }
    }
  }

  // TODO Verify this statement: Can't delete the table if it's the only instance of the table that exists
  // TODO Verify this statement: Can't delete the table if it's published
  // TODO Verify this statement: Maybe can delete the table if it's a snapshot?
  test("Universe can delete a table by adding row to pending_table_drops") {
    withDb() { conn =>
      val (_, copyInfo, sLoader) = createTable(conn)
      sLoader.drop(copyInfo)
      val dataTableName = copyInfo.dataTableName
      // Check to see if dropped table is in pending drop table operations collection
      val tableName = fetchColumnFromTable(connection = conn, Map(
        "tableName" -> "pending_table_drops",
        "columnName" -> "table_name",
        "whereClause" -> s"table_name = '$dataTableName'"
      ))
      assert(tableName.length > 0, s"Expected to find $dataTableName in the pending_table_drops table")
    }
  }

  test("Universe can delete a table that is referenced by pending_table_drops") {
    withDb() { conn =>
      val (_, copyInfo, sLoader) = createTable(conn)
      sLoader.drop(copyInfo)
      val dataTableName = copyInfo.dataTableName
      updateColumnValueInTable(connection = conn, options = Map(
        "tableName" -> "pending_table_drops",
        "columnName" -> "queued_at",
        "columnValue" -> "now() - ('2 day' :: INTERVAL)",
        "whereClause" -> s"table_name = '$dataTableName'"
      ))
      val deletedSomeTables = new SqlTableCleanup(conn).cleanupPendingDrops()
      assert(deletedSomeTables, "Expected to have deleted at least one row from pending_table_drops")
      val tableName = fetchColumnFromTable(connection = conn, Map(
        "tableName" -> "pending_table_drops",
        "columnName" -> "table_name",
        "whereClause" -> s"table_name = '$dataTableName'"
      ))
      assert(tableName.length == 0, s"Expected NOT to find $dataTableName in the pending_table_drops table")
      val publicTableName = fetchColumnFromTable(connection = conn, Map(
        "tableName" -> "pg_tables",
        "columnName" -> "tablename",
        "whereClause" -> s"schemaname = 'public' AND tablename = '$dataTableName'"
      ))
      assert(publicTableName.length == 0, "Expected NOT to find $dataTableName in the pg_tables table")
    }
  }

  // TODO Is this deleting a "working copy" ?
  // TODO Is this deleting a "snapshot" ?
  // TODO Does it matter if the dataset has been published or not?
  test("Universe can delete a published copy") {

  }
}
