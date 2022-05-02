package com.socrata.pg.store

import java.sql.{Connection, SQLException}
import com.rojoma.simplearm.v2._

import javax.sql.DataSource


class SqlIndexCleanup() extends IndexCleanup {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlIndexCleanup])

  val claimSql =
    s"""
        UPDATE pending_index_drops SET claimant=?, claim_at=now(), retry_count=retry_count+1 WHERE id =
               (SELECT id FROM pending_index_drops WHERE claimant is null OR claim_at < now() - interval '1 hour' ORDER BY claim_at LIMIT 1) RETURNING id, name"""

  /**
   * return whether work exists
   */
  def cleanupPendingDrops(dataSource: DataSource, host: String): Boolean = {
    using(dataSource.getConnection) { conn =>
      conn.setAutoCommit(true)
      using(conn.createStatement(), conn.prepareStatement(claimSql)) { (dropStmt, claimStmt) =>
        claimStmt.setString(1, host)
        using(claimStmt.executeQuery()) { rs =>
          if (rs.next()) {
            val id = rs.getLong("id")
            val name = rs.getString("name")
            dropStmt.setQueryTimeout(60)
            log.info("Physically dropping index " + name)
            try {
              dropStmt.execute(s"""DROP INDEX CONCURRENTLY IF EXISTS "${name}"; DELETE FROM pending_index_drops WHERE id=${id}""")
              true
            } catch {
              case ex: SQLException =>
                conn.clearWarnings()
                log.error(s"sql error dropping index $name ${ex.toString}")
                true
            }
          } else {
            false
          }
        }
      }
    }
  }
}
