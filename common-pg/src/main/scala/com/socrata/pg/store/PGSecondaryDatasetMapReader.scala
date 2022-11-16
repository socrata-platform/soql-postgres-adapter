package com.socrata.pg.store

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{CopyId, DatasetId, RollupName}
import com.socrata.datacoordinator.truth.metadata.sql.{BasePostgresDatasetMapReader, PostgresDatasetMapReader}
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetInfo, LifecycleStage, TypeNamespace}
import com.socrata.datacoordinator.util.TimingReport
import org.joda.time.DateTime

import java.sql.Connection
import scala.reflect.ClassTag

trait LocalRollupReaderOverride[CT] {
  self: BasePostgresDatasetMapReader[CT] =>

  def localRollupsQuery = "SELECT system_id, name, soql, table_name FROM rollup_map WHERE copy_system_id = ?"
  override def rollups(copyInfo: CopyInfo): Seq[LocalRollupInfo] = {
    using(conn.prepareStatement(localRollupsQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("rollups", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) {
        Iterator.continually(_)
          .takeWhile(_.next())
          .map(rs=>
            new LocalRollupInfo(
              copyInfo,
              new RollupName(rs.getString("name")),
              rs.getString("soql"),
              rs.getString("table_name"),
              RollupId(rs.getLong("system_id"))
            )
          ).toList
      }
    }
  }

  def localRollupQuery = "SELECT name,system_id, soql, table_name FROM rollup_map WHERE copy_system_id = ? AND name = ?"

  override def rollup(copyInfo: CopyInfo, name: RollupName): Option[LocalRollupInfo] = {
    using(conn.prepareStatement(localRollupQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, name.underlying)
      using(t("rollup", "copy_id" -> copyInfo.systemId, "name" -> name)(stmt.executeQuery())) { rs =>
        if (rs.next()) {
          Some(new LocalRollupInfo(copyInfo, new RollupName(rs.getString("name")), rs.getString("soql"), rs.getString("table_name"), RollupId(rs.getLong("system_id"))))
        } else {
          None
        }
      }
    }
  }

  def getRollupCopiesRelatedToCopyQuery =
    """select dataset_map.system_id           as dataset_map_system_id,
       dataset_map.next_counter_value  as dataset_map_next_counter_value,
       dataset_map.latest_data_version as dataset_map_latest_data_version,
       dataset_map.locale_name         as dataset_map_locale_name,
       dataset_map.obfuscation_key     as dataset_map_obfuscation_key,
       dataset_map.resource_name       as dataset_map_resource_name,
       copy_map.system_id as copy_map_system_id,
       copy_map.copy_number as copy_map_copy_number,
       copy_map.lifecycle_stage :: TEXT as copy_map_lifecycle_stage,
       copy_map.data_version as copy_map_data_version,
       copy_map.data_shape_version as copy_map_data_shape_version,
       copy_map.last_modified as copy_map_last_modified,
       copy_map_table_modifiers.table_modifier as copy_map_table_modifiers_table_modifier
from rollup_relationship_map
         join rollup_map on rollup_relationship_map.rollup_system_id = rollup_map.system_id
         join copy_map on rollup_map.copy_system_id = copy_map.system_id
         left join copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
         join dataset_map on copy_map.dataset_system_id = dataset_map.system_id
where referenced_copy_system_id = ?"""

  def getRollupCopiesRelatedToCopy(copyInfo: CopyInfo): Set[CopyInfo] = {
    using(conn.prepareStatement(getRollupCopiesRelatedToCopyQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("getRollupCopiesRelatedToCopy", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) {
        Iterator.continually(_)
          .takeWhile(_.next())
          .map(rs=>
            CopyInfo(
              DatasetInfo(new DatasetId(rs.getLong("dataset_map_system_id")), rs.getLong("dataset_map_next_counter_value"), rs.getLong("dataset_map_latest_data_version"), rs.getString("dataset_map_locale_name"), rs.getBytes("dataset_map_obfuscation_key"), Option(rs.getString("dataset_map_resource_name"))),
              new CopyId(rs.getLong("copy_map_system_id")),
              rs.getLong("copy_map_copy_number"),
              LifecycleStage.valueOf(rs.getString("copy_map_lifecycle_stage")),
              rs.getLong("copy_map_data_version"),
              rs.getLong("copy_map_data_shape_version"),
              new DateTime(rs.getTimestamp("copy_map_last_modified").getTime),
              Some(rs.getLong("copy_map_table_modifiers_table_modifier"))
            )
          ).toList.toSet
      }
    }
  }

  def getRollupsRelatedToCopyQuery =
    """select
       rollup_map.system_id            as rollup_map_system_id,
       rollup_map.name                 as rollup_map_name,
       rollup_map.soql                 as rollup_map_soql,
       rollup_map.table_name           as rollup_map_table_name,
       dataset_map.system_id           as dataset_map_system_id,
       dataset_map.next_counter_value  as dataset_map_next_counter_value,
       dataset_map.latest_data_version as dataset_map_latest_data_version,
       dataset_map.locale_name         as dataset_map_locale_name,
       dataset_map.obfuscation_key     as dataset_map_obfuscation_key,
       dataset_map.resource_name       as dataset_map_resource_name,
       copy_map.system_id as copy_map_system_id,
       copy_map.copy_number as copy_map_copy_number,
       copy_map.lifecycle_stage :: TEXT as copy_map_lifecycle_stage,
       copy_map.data_version as copy_map_data_version,
       copy_map.data_shape_version as copy_map_data_shape_version,
       copy_map.last_modified as copy_map_last_modified,
       copy_map_table_modifiers.table_modifier as copy_map_table_modifiers_table_modifier
from rollup_relationship_map
         join rollup_map on rollup_relationship_map.rollup_system_id = rollup_map.system_id
         join copy_map on rollup_map.copy_system_id = copy_map.system_id
         left join copy_map_table_modifiers ON copy_map.system_id = copy_map_table_modifiers.copy_system_id
         join dataset_map on copy_map.dataset_system_id = dataset_map.system_id
where referenced_copy_system_id = ?"""

  def getRollupsRelatedToCopy(copyInfo: CopyInfo): Set[LocalRollupInfo] = {
    using(conn.prepareStatement(getRollupsRelatedToCopyQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("getRollupsRelatedToCopy", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) {
        Iterator.continually(_)
          .takeWhile(_.next())
          .map(rs=>
            new LocalRollupInfo(
              CopyInfo(
                DatasetInfo(new DatasetId(rs.getLong("dataset_map_system_id")), rs.getLong("dataset_map_next_counter_value"), rs.getLong("dataset_map_latest_data_version"), rs.getString("dataset_map_locale_name"), rs.getBytes("dataset_map_obfuscation_key"), Option(rs.getString("dataset_map_resource_name"))),
                new CopyId(rs.getLong("copy_map_system_id")),
                rs.getLong("copy_map_copy_number"),
                LifecycleStage.valueOf(rs.getString("copy_map_lifecycle_stage")),
                rs.getLong("copy_map_data_version"),
                rs.getLong("copy_map_data_shape_version"),
                new DateTime(rs.getTimestamp("copy_map_last_modified").getTime),
                Some(rs.getLong("copy_map_table_modifiers_table_modifier"))
              ),
              new RollupName(rs.getString("rollup_map_name")),
              rs.getString("rollup_map_soql"),
              rs.getString("rollup_map_table_name"),
              RollupId(rs.getLong("rollup_map_system_id")))
          ).toList.toSet
      }
    }
  }

  def nextSequenceValue = "select nextval(?)"
  def getNextSequence(sequenceName: String): Option[Long] = {
    using(conn.prepareStatement(nextSequenceValue)) { stmt =>
      stmt.setString(1, sequenceName)
      using(t("getNextSequence", "sequence" -> sequenceName)(stmt.executeQuery())) { rs =>
        if (rs.next()) Some(rs.getLong(1)) else None
      }
    }
  }

  def getNextRollupTableNameSequence = getNextSequence(RollupManager.tableNameSequenceIdentifier).getOrElse(throw new IllegalStateException(s"Could not get next value of sequence '${RollupManager.tableNameSequenceIdentifier}'"))

}


class PGSecondaryDatasetMapReader[CT](conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport)
  extends PostgresDatasetMapReader[CT](conn, tns, timingReport)
    with LocalRollupReaderOverride[CT] {
  val idFromName =
    """SELECT dataset_system_id, disabled
      |  FROM dataset_internal_name_map
      | WHERE dataset_internal_name = ?
    """.stripMargin

  def datasetIdForInternalName(datasetInternalName: String, checkDisabled: Boolean = false): Option[DatasetId] = {
    using(conn.prepareStatement(idFromName)) { stmt =>
      stmt.setString(1, datasetInternalName)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) {
          if (checkDisabled && rs.getDate("disabled") != null) {
            None
          } else {
            Option(new DatasetId(rs.getLong("dataset_system_id")))
          }
        } else {
          None
        }
      }
    }
  }
}
