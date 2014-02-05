package com.socrata.pg.store

import com.rojoma.simplearm.util._
import java.sql.Connection
import com.socrata.datacoordinator.id.DatasetId

class PostgresDatasetInternalNameMapWriter(val conn: Connection) {

  def createQuery_internalNameMap = "INSERT INTO dataset_internal_name_map (dataset_internal_name, dataset_system_id) VALUES (?, ?)"

  def create(datasetInternalName: String, datasetSystemId: DatasetId) {
    using(conn.prepareStatement(createQuery_internalNameMap)) { stmt =>
      stmt.setString(1, datasetInternalName)
      stmt.setLong(2, datasetSystemId.underlying)
      stmt.execute()
    }
  }

}
