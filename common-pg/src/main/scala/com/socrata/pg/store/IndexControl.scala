package com.socrata.pg.store

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.soql.environment.ColumnName

trait IndexControl[CT] { this: PGSecondaryDatasetMapWriter[CT] =>

  private val upsertSql = """
      INSERT INTO index_directives(dataset_system_id, field_name_casefolded, directives)
             VALUES (?, ?, ?)
          ON CONFLICT(dataset_system_id, field_name_casefolded)
          DO UPDATE SET directives = ?,
                        updated_at = now(),
                        deleted_at = null
    """

  private val deleteSql = """
      UPDATE index_directives SET deleted_at = now()
       WHERE dataset_system_id = ?
         AND field_name_casefolded = ?
    """

  def createIndexDirectives(datasetSystemId: DatasetId, column: ColumnName, directives: String): Unit = {
    using(conn.prepareStatement(upsertSql)) { stmt =>
      stmt.setLong(1, datasetSystemId.underlying)
      stmt.setString(2, column.caseFolded)
      stmt.setString(3, directives)
      stmt.setString(4, directives)
      stmt.execute()
    }
  }

  def deleteIndexDirectives(datasetSystemId: DatasetId, column: ColumnName): Unit = {
    using(conn.prepareStatement(deleteSql)) { stmt =>
      stmt.setLong(1, datasetSystemId.underlying)
      stmt.setString(2, column.caseFolded)
      stmt.execute()
    }
  }
}
