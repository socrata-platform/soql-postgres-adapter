package com.socrata.pg.store

import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{CopyId, DatasetId}
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.DatabaseInReadOnlyMode
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, TypeNamespace, CopyInfo}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.util.TimingReport
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.Connection
import org.postgresql.util.PSQLException


class PGSecondaryDatasetMapWriter[CT](override val conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport, override val obfuscationKeyGenerator: () => Array[Byte], override val initialCounterValue: Long) extends
  PostgresDatasetMapWriter(conn, tns, timingReport, obfuscationKeyGenerator, initialCounterValue) with Logging {

  private val createQuery_internalNameMap = "INSERT INTO dataset_internal_name_map (dataset_internal_name, dataset_system_id) VALUES (?, ?)"

  private val deleteQuery_internalNameMap = "DELETE FROM dataset_internal_name_map where dataset_system_id = ?"

  override def delete(tableInfo: DatasetInfo) {
    deleteInternalNameMapping(tableInfo.systemId)
    super.delete(tableInfo)
  }

  def createInternalNameMapping(datasetInternalName: String, datasetSystemId: DatasetId) {
    using(conn.prepareStatement(createQuery_internalNameMap)) { stmt =>
      stmt.setString(1, datasetInternalName)
      stmt.setLong(2, datasetSystemId.underlying)
      stmt.execute()
    }
  }

  def deleteInternalNameMapping(datasetSystemId: DatasetId) {
    using(conn.prepareStatement(deleteQuery_internalNameMap)) { stmt =>
      stmt.setLong(1, datasetSystemId.underlying)
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

    dropRollup(copyInfo, None) // drop all related rollups

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

  def allocateCopyId(): CopyId = {
    using(conn.prepareStatement("select nextval('copy_map_system_id_seq')")) { stmt =>
      val rs = stmt.executeQuery()
      if (rs.next()) new com.socrata.datacoordinator.id.CopyId(rs.getLong(1))
      else throw new Exception("cannot get new copy id from sequence")
    }
  }
}
