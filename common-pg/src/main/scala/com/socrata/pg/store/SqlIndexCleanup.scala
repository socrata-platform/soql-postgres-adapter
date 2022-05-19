package com.socrata.pg.store

import java.sql.SQLException
import com.rojoma.simplearm.v2._
import com.socrata.pg.query.QueryResult.QueryRuntimeError

import javax.sql.DataSource


class SqlIndexCleanup() extends IndexCleanup {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SqlIndexCleanup])

  private val claimSql =
    s"""
        UPDATE pending_index_drops SET claimant=?, claim_at=now(), retry_count=retry_count+1
         WHERE id = (SELECT id
                       FROM pending_index_drops
                      WHERE claimant is null OR claim_at < now() - interval '1 hour'
                      ORDER BY claim_at LIMIT 1) RETURNING id, name"""

  /**
   * This function does not share connection with others because dropping index CONCURRENTLY must be in AutoCommit mode.
   * return: true if there is work so that caller can call work immediately without pause
   */
  def cleanupPendingDrops(dataSource: DataSource, claimant: String): Boolean = {
    using(dataSource.getConnection) { conn =>
      conn.setAutoCommit(true)
      using(conn.createStatement(), conn.prepareStatement(claimSql)) { (dropStmt, claimStmt) =>
        claimStmt.setString(1, claimant)
        using(claimStmt.executeQuery()) { rs =>
          if (rs.next()) {
            val id = rs.getLong("id")
            val name = rs.getString("name")
            dropStmt.setQueryTimeout(60/*second*/)
            log.info(s"Physically dropping index $name")
            try {
              dropStmt.execute(s"""DROP INDEX CONCURRENTLY IF EXISTS "${name}"; DELETE FROM pending_index_drops WHERE id=${id}""")
              true
            } catch {
              case ex: SQLException =>
                QueryRuntimeError(ex) match {
                  case None =>
                    log.error(s"SQL Exception drop index $name (${ex.getSQLState})", ex)
                  case Some(e) =>
                    log.warn(s"SQL Exception drop index $name ${e.getClass.getSimpleName} (${ex.getSQLState})")
                }
                conn.clearWarnings() // reset connection on the basis of autoCommit being true
                true // failed, but there is work
            }
          } else {
            false // no work, caller might sleep a bit
          }
        }
      }
    }
  }
}
