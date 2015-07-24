package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId

class PGSecondaryDatasetMapReader(val conn: Connection) {
  val idFromName =
    """SELECT dataset_system_id
      |  FROM dataset_internal_name_map
      | WHERE dataset_internal_name = ?
    """.stripMargin
  def datasetIdForInternalName(datasetInternalName: String): Option[DatasetId] = {
    using(conn.prepareStatement(idFromName)) { stmt =>
      stmt.setString(1, datasetInternalName)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) {
          Option(new DatasetId(rs.getLong("dataset_system_id")))
        } else {
          None
        }
      }
    }
  }
}
