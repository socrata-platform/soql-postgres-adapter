package com.socrata.pg.store

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{CopyId, DatasetId, RollupName}
import com.socrata.datacoordinator.id.sql._
import com.socrata.datacoordinator.truth.DatabaseInReadOnlyMode
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo, TypeNamespace}
import com.socrata.datacoordinator.truth.metadata.sql.{BasePostgresDatasetMapReader, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.pg.store.RollupManager.logger
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
  PostgresDatasetMapWriter(conn, tns, timingReport, obfuscationKeyGenerator, initialCounterValue, initialVersion) with LocalRollupReaderOverride[CT] {
  import PGSecondaryDatasetMapWriter.logger

  private val createQueryInternalNameMap =
    """INSERT INTO dataset_internal_name_map
      |(dataset_internal_name, dataset_system_id)
      |VALUES (?, ?)
    """.stripMargin
  private val deleteQueryInternalNameMap = "DELETE FROM dataset_internal_name_map where dataset_system_id = ?"
  private val enableQueryInternalNameMap = "UPDATE dataset_internal_name_map SET disabled = ? WHERE dataset_system_id = ?"

  override def delete(tableInfo: DatasetInfo): Unit = {
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

    deleteRollupRelationships(copyInfo)
    deleteRollupRelationshipByRollupMapCopyInfo(copyInfo)
    dropRollup(copyInfo, None) // drop all related rollups metadata
    dropIndexDirectives(copyInfo)
    dropIndex(copyInfo, None)

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

  def insertLocalRollupQuery = "INSERT INTO rollup_map (name, copy_system_id, soql, table_name) VALUES (?, ?, ?, ?) returning system_id"
  def updateLocalRollupQuery = "UPDATE rollup_map SET soql = ? WHERE name = ? AND copy_system_id = ? returning system_id,table_name"
  def createOrUpdateRollupSecondary(copyInfo: CopyInfo, name: RollupName, soql: String, rawSoql: Option[String]): Option[LocalRollupInfo] = {
    rollup(copyInfo, name) match {
      case Some(_) =>
        using(conn.prepareStatement(updateLocalRollupQuery)) { stmt =>
          stmt.setString(1, soql)
          stmt.setString(2, name.underlying)
          stmt.setLong(3, copyInfo.systemId.underlying)
          t("create-or-update-rollup", "action" -> "update", "copy-id" -> copyInfo.systemId, "name" -> name)(
            using(stmt.executeQuery()){resultSet =>
              if(resultSet.next()){
                 Some(new LocalRollupInfo(copyInfo, name, soql, resultSet.getString("table_name"),RollupId(resultSet.getLong("system_id"))))
              }else None
            }
          )
        }
      case None =>

        val tableName = LocalRollupInfo.tableName(copyInfo, name,getNextRollupTableNameSequence.toString)
        using(conn.prepareStatement(insertLocalRollupQuery)) { stmt =>
          stmt.setString(1, name.underlying)
          stmt.setLong(2, copyInfo.systemId.underlying)
          stmt.setString(3, soql)
          stmt.setString(4, tableName)
          t("create-or-update-rollup", "action" -> "insert", "copy-id" -> copyInfo.systemId, "name" -> name)(
            using(stmt.executeQuery()){resultSet =>
              if(resultSet.next()){
                Some(new LocalRollupInfo(copyInfo, name, soql, tableName,RollupId(resultSet.getLong("system_id"))))
              }else None
            }
          )
        }
    }
  }
  def createRollupRelationship = "insert into rollup_relationship_map (rollup_system_id, referenced_copy_system_id) values (?,?) on conflict do nothing"
  def createRollupRelationship(rollupInfo: LocalRollupInfo, relatedCopyInfo: CopyInfo): Unit ={
    using(conn.prepareStatement(createRollupRelationship)) { stmt =>
      stmt.setLong(1, rollupInfo.systemId.underlying)
      stmt.setLong(2, relatedCopyInfo.systemId.underlying)
      t("create-rollup-relationship", "rollupInfo" -> rollupInfo.systemId, "referencedCopyId" -> relatedCopyInfo.systemId.underlying)(
        stmt.executeUpdate()
      )
    }
  }

  def deleteRollupRelationshipsRollupQuery = "delete from rollup_relationship_map where rollup_system_id = ?"
  def deleteRollupRelationships(rollupInfo: LocalRollupInfo): Unit ={
    using(conn.prepareStatement(deleteRollupRelationshipsRollupQuery)) { stmt =>
      stmt.setLong(1, rollupInfo.systemId.underlying)
      t("delete-rollup-relationship", "rollupInfo" -> rollupInfo.systemId)(
        stmt.executeUpdate()
      )
    }
  }

  def deleteRollupRelationshipsCopyQuery = "delete from rollup_relationship_map where referenced_copy_system_id = ?"
  def deleteRollupRelationships(copyInfo: CopyInfo): Unit ={
    using(conn.prepareStatement(deleteRollupRelationshipsCopyQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      t("delete-rollup-relationship", "copyInfo" -> copyInfo.systemId)(
        stmt.executeUpdate()
      )
    }
  }

  def deleteRollupRelationshipByRollupMapCopyInfoQuery = "delete from rollup_relationship_map where rollup_system_id in (select system_id from rollup_map where copy_system_id=?)"

  def deleteRollupRelationshipByRollupMapCopyInfo(copyInfo: CopyInfo): Unit = {
    using(conn.prepareStatement(deleteRollupRelationshipByRollupMapCopyInfoQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      t("delete-rollup-relationship-by-rollup-map-copy-info", "copyInfo" -> copyInfo.systemId)(
        stmt.executeUpdate()
      )
    }
  }
  def transferLocalRollupsQuery = "UPDATE rollup_map SET copy_system_id = ? WHERE copy_system_id = ? and name = ?"
  def transferRollup(from: LocalRollupInfo, to: CopyInfo) {
    if(from.copyInfo.systemId != to.systemId) {
      using(conn.prepareStatement(transferLocalRollupsQuery)) { stmt =>
        stmt.setLong(1, from.copyInfo.systemId.underlying)
        stmt.setLong(2, to.systemId.underlying)
        stmt.setString(3, from.name.underlying)
        if(t("transfer-rollups", "from" -> from.copyInfo.systemId, "to" -> to.systemId, "name" -> from.name)(stmt.executeUpdate()) != 1) {
          throw new Exception(s"Told to transfer a rollup that doesn't seem to exist: ${from.copyInfo}")
        }
      }
    }
  }

  def changeRollupTableNameQuery = "UPDATE rollup_map SET table_name = ? WHERE copy_system_id = ? and name = ?"
  def changeRollupTableName(ri: LocalRollupInfo, name: String): LocalRollupInfo = {
    logger.info(s"Renaming rollup table ${ri.tableName} to ${name}")
    using(conn.prepareStatement(changeRollupTableNameQuery)) { stmt =>
      stmt.setString(1, name)
      stmt.setLong(2, ri.copyInfo.systemId.underlying)
      stmt.setString(3, ri.name.underlying)
      if(t("change-rollup-table-name", "copy-id" -> ri.copyInfo.systemId, "name" -> ri.name, "new-table" -> name)(stmt.executeUpdate()) != 1) {
        throw new Exception(s"Told to change a rollup's table name that doesn't seem to exist: ${ri.copyInfo.systemId} ${ri.name}")
      }
      ri.updateName(name)
    }
  }

  def dropRollup(ri: LocalRollupInfo) {
    deleteRollupRelationships(ri)
    dropRollup(ri.copyInfo, Some(ri.name))
  }
}
