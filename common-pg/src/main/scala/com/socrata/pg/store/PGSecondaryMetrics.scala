package com.socrata.pg.store

import java.sql.Connection

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.datacoordinator.secondary.SecondaryMetric

class PGSecondaryMetrics(conn: Connection) {
  def dataset(datasetId: DatasetId): Option[SecondaryMetric] = {
    using(conn.prepareStatement(
      """SELECT sum(pg_total_relation_size(oid)) AS total_size_bytes
        |  FROM pg_class
        |  WHERE relkind = 'r'
        |    AND relname LIKE ?
      """.stripMargin)) { stmt =>
      val like = "t" + datasetId.underlying + "\\_%"
      stmt.setString(1, like)
      using(stmt.executeQuery()) { rs =>
        if (rs.next()) Some(SecondaryMetric(rs.getLong("total_size_bytes")))
        else None
      }
    }
  }
}
