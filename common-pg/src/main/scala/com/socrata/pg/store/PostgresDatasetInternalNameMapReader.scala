package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.DatasetId

class PostgresDatasetInternalNameMapReader(val conn: Connection) {

  def datasetIdForInternalName(datasetInternalName: String): Option[DatasetId] = {
    using(conn.prepareStatement("SELECT dataset_system_id FROM dataset_internal_name_map WHERE dataset_internal_name = ?")) { stmt =>
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
