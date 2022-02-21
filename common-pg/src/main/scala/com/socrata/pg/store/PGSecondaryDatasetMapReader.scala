package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{DatasetId, RollupName}
import com.socrata.datacoordinator.util.TimingReport
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, BasePostgresDatasetMapReader}
import com.socrata.datacoordinator.truth.metadata.{TypeNamespace, CopyInfo}
import com.socrata.soql.types.SoQLType

trait LocalRollupReaderOverride[CT] { self: BasePostgresDatasetMapReader[CT] =>
  def localRollupsQuery = "SELECT name, soql, table_name FROM rollup_map WHERE copy_system_id = ?"
  override def rollups(copyInfo: CopyInfo): Seq[LocalRollupInfo] = {
    using(conn.prepareStatement(localRollupsQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      using(t("rollups", "copy_id" -> copyInfo.systemId)(stmt.executeQuery())) { rs =>
        val res = Vector.newBuilder[LocalRollupInfo]
        while(rs.next()) {
          res += new LocalRollupInfo(copyInfo, new RollupName(rs.getString("name")), rs.getString("soql"), rs.getString("table_name"))
        }
        res.result()
      }
    }
  }

  def localRollupQuery = "SELECT soql, table_name FROM rollup_map WHERE copy_system_id = ? AND name = ?"
  override def rollup(copyInfo: CopyInfo, name: RollupName): Option[LocalRollupInfo] = {
    using(conn.prepareStatement(localRollupQuery)) { stmt =>
      stmt.setLong(1, copyInfo.systemId.underlying)
      stmt.setString(2, name.underlying)
      using(t("rollup", "copy_id" -> copyInfo.systemId,"name" -> name)(stmt.executeQuery())) { rs =>
        if(rs.next()) {
          Some(new LocalRollupInfo(copyInfo, name, rs.getString("soql"), rs.getString("table_name")))
        } else {
          None
        }
      }
    }
  }
}

class PGSecondaryDatasetMapReader[CT](conn: Connection, tns: TypeNamespace[CT], timingReport: TimingReport)
    extends PostgresDatasetMapReader[CT](conn, tns, timingReport)
    with LocalRollupReaderOverride[CT]
{
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
