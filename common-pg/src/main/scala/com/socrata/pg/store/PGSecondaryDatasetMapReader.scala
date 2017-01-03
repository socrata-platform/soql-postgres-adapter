package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId

class PGSecondaryDatasetMapReader(val conn: Connection) {
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
