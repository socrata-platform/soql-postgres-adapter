package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo}
import com.socrata.datacoordinator.truth.DatabaseInReadOnlyMode
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.{Statement, Connection}
import org.postgresql.util.PSQLException

class PGSecondaryDatasetMapWriter(val conn: Connection, val obfuscationKeyGenerator: () => Array[Byte], val initialCounterValue: Long) extends Logging {

  def createQuery_internalNameMap = "INSERT INTO dataset_internal_name_map (dataset_internal_name, dataset_system_id) VALUES (?, ?)"

  def createInternalNameMapping(datasetInternalName: String, datasetSystemId: DatasetId) {
    using(conn.prepareStatement(createQuery_internalNameMap)) { stmt =>
      stmt.setString(1, datasetInternalName)
      stmt.setLong(2, datasetSystemId.underlying)
      stmt.execute()
    }
  }


  def createDatasetOnlyQuery_tableMap = "INSERT INTO dataset_map (next_counter_value, locale_name, obfuscation_key) VALUES (?, ?, ?) RETURNING system_id"

  /**
   * Creates a dataset_map entry with no copy info.
   */
  def createDatasetOnly(localeName: String): DatasetId = {
    using(conn.prepareStatement(createDatasetOnlyQuery_tableMap)) { stmt =>
      stmt.setLong(1, initialCounterValue)
      stmt.setString(2, localeName)
      stmt.setBytes(3, obfuscationKeyGenerator())
      try {
        using(stmt.executeQuery()) { rs =>
          val returnedSomething = rs.next()
          assert(returnedSomething, "INSERT didn't return a system ID?")
          rs.getDatasetId(1)
        }
      } catch {
        case e: PSQLException if isReadOnlyTransaction(e) =>
          logger.warn("Create dataset failed due to read-only txn; abandoning")
          throw new DatabaseInReadOnlyMode(e)
      }
    }
  }

  def deleteCopyQuery_columnMap = "DELETE FROM column_map WHERE copy_system_id = ?"
  def deleteCopyQuery_copyMap = "DELETE FROM copy_map WHERE system_id = ?"

  /**
   * Deletes/drops a copy entirely, including column_map, copy_info and the actual table.
   */
  def deleteCopy(copyInfo: CopyInfo) {
    using(conn.prepareStatement(deleteCopyQuery_columnMap)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.executeUpdate()
    }

    using(conn.prepareStatement(deleteCopyQuery_copyMap)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.executeUpdate()
    }

    using(conn.createStatement()) { stmt =>
      // this doesn't really belong in the MapWriter, but it isn't yet worth creating
      // somewhere else it put it...

      // Doing the drop directly here is potentially problematic from a
      // locking perspective since it requires an ACCESS EXCLUSIVE lock, but
      // we have a very similar problem for ALTER TABLE given our current lack
      // of any sort of working copy workflow.  The couple of possible
      // hackish workarounds that we could use here ended up looking ugly
      // and complicating the logic... possibly to be revisited.
      stmt.execute("DROP TABLE IF EXISTS " + copyInfo.dataTableName)
    }
  }

  def createFullTextSearchIndex(copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]], repFor: (ColumnInfo[SoQLType]) => SqlColumnRep[SoQLType, SoQLValue]) {
    PostgresUniverseCommon.searchVector(schema.values.map(repFor).toSeq) match {
      case Some(allColumnsVector) =>
        val ttable = copyInfo.dataTableName
        using(conn.createStatement()) { (stmt: Statement) =>
          stmt.execute(s"DROP INDEX IF EXISTS idx_search_${ttable}")
          stmt.execute(s"CREATE INDEX idx_search_${ttable} on ${ttable} USING GIN ($allColumnsVector)")
        }
      case None => // nothing to do
    }
  }

  def readOnlySqlTransactionState = "25006"

  def isReadOnlyTransaction(e: PSQLException): Boolean =
    e.getSQLState == readOnlySqlTransactionState

}
