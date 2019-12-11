package com.socrata.pg.store

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{CopyId, DatasetId}
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.DatabaseInReadOnlyMode
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo, TypeNamespace}
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter
import com.socrata.datacoordinator.util.TimingReport
import com.typesafe.scalalogging.Logger
import java.sql.{Connection, Timestamp}

import org.postgresql.util.PSQLException

object PGSecondaryDatasetMapWriter {
  private val logger = Logger[PGSecondaryDatasetMapWriter[_]]
}

class PGSecondaryDatasetMapWriter[CT](override val conn: Connection,
                                      tns: TypeNamespace[CT],
                                      timingReport: TimingReport,
                                      override val obfuscationKeyGenerator: () => Array[Byte],
                                      override val initialCounterValue: Long,
                                      val initialVersion: Long) extends
  PostgresDatasetMapWriter(conn, tns, timingReport, obfuscationKeyGenerator, initialCounterValue, initialVersion) {
  import PGSecondaryDatasetMapWriter.logger

  private val createQueryInternalNameMap =
    """INSERT INTO dataset_internal_name_map
      |(dataset_internal_name, dataset_system_id)
      |VALUES (?, ?)
    """.stripMargin
  private val deleteQueryInternalNameMap = "DELETE FROM dataset_internal_name_map where dataset_system_id = ?"
  private val enableQueryInternalNameMap = "UPDATE dataset_internal_name_map SET disabled = ? WHERE dataset_system_id = ?"
  private val deleteIndexDirectives = "DELETE FROM index_directives where dataset_system_id = ?"

  override def delete(tableInfo: DatasetInfo): Unit = {
    deleteIndexDirectives(tableInfo.systemId)
    deleteInternalNameMapping(tableInfo.systemId)
    super.delete(tableInfo)
  }

  def createInternalNameMapping(datasetInternalName: String, datasetSystemId: DatasetId): Unit = {
    using(conn.prepareStatement(createQueryInternalNameMap)) { stmt =>
      stmt.setString(1, datasetInternalName)
      stmt.setLong(2, datasetSystemId.underlying)
      stmt.execute()
    }
  }

  def deleteInternalNameMapping(datasetSystemId: DatasetId): Unit = {
    using(conn.prepareStatement(deleteQueryInternalNameMap)) { stmt =>
      stmt.setLong(1, datasetSystemId.underlying)
      stmt.execute()
    }
  }

  def deleteIndexDirectives(datasetSystemId: DatasetId): Unit = {
    using(conn.prepareStatement(deleteIndexDirectives)) { stmt =>
      stmt.setLong(1, datasetSystemId.underlying)
      stmt.execute()
    }
  }

  def disableDataset(datasetSystemId: DatasetId): Unit = {
    enableDataset(datasetSystemId, false)
  }

  def enableDataset(datasetSystemId: DatasetId, enabled: Boolean = true): Unit = {
    using(conn.prepareStatement(enableQueryInternalNameMap)) { stmt =>
      val timestamp = if (enabled) null else new Timestamp(System.currentTimeMillis)
      stmt.setTimestamp(1, timestamp)
      stmt.setLong(2, datasetSystemId.underlying)
      stmt.execute()
    }
  }

  val createDatasetOnlyQueryTableMap =
    """INSERT INTO dataset_map
      |(next_counter_value, locale_name, obfuscation_key)
      |VALUES (?, ?, ?)
      |RETURNING system_id
    """.stripMargin

  /**
   * Creates a dataset_map entry with no copy info.
   */
  def createDatasetOnly(localeName: String): DatasetId = {
    using(conn.prepareStatement(createDatasetOnlyQueryTableMap)) { stmt =>
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

  val deleteCopyQueryColumnMap = "DELETE FROM column_map WHERE copy_system_id = ?"
  val deleteCopyQueryCopyMapTableModifiers = "DELETE FROM copy_map_table_modifiers WHERE copy_system_id = ?"
  val deleteCopyQueryCopyMap = "DELETE FROM copy_map WHERE system_id = ?"

  /**
   * Deletes/drops a copy entirely, including column_map, copy_info and the actual table.
   */
  def deleteCopy(copyInfo: CopyInfo): Unit = {

    dropRollup(copyInfo, None) // drop all related rollups metadata

    using(conn.prepareStatement(deleteCopyQueryColumnMap)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.executeUpdate()
    }

    using(conn.prepareStatement(deleteCopyQueryCopyMapTableModifiers)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.executeUpdate()
    }

    using(conn.prepareStatement(deleteCopyQueryCopyMap)) { stmt =>
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
      if (rs.next()) {
        new com.socrata.datacoordinator.id.CopyId(rs.getLong(1))
      } else {
        throw new Exception("cannot get new copy id from sequence")
      }
    }
  }
}
