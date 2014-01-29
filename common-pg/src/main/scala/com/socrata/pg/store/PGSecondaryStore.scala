package com.socrata.pg.store

import com.socrata.datacoordinator.truth.loader.sql.{DataSqlizer, AbstractRepBasedDataSqlizer}
import java.sql.Connection
import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import scala.Some

/**
 *  Secondary Postgres Implementation
 */
class PGSecondaryStore[CT, CV](tableName: String,
                               datasetContext: RepBasedSqlDatasetContext[CT, CV],
                               conn:Connection)
  extends AbstractRepBasedDataSqlizer(tableName, datasetContext) {
  type PreloadStatistics = Long

  /**
   * Implement or override to dynamically choose a table space
   */
  private def postgresTablespaceSuffixFor(tablespace: Option[String]): String =
    tablespace match {
      case Some(ts) =>
        " TABLESPACE " + ts
      case None =>
        ""
    }

  /**
   * None == table did not exist; Some(None) == default tablespace; Some(Some(ts)) == specific tablespace
   */
  def tablespaceOfTable(tableName: String): Option[Option[String]] =
    using(conn.prepareStatement("SELECT tablespace FROM pg_tables WHERE schemaname = ? AND tablename = ?")) { stmt =>
      stmt.setString(1, "public")
      stmt.setString(2, tableName)
      using(stmt.executeQuery()) { rs =>
        if(rs.next()) {
          Some(Option(rs.getString("tablespace")))
        } else {
          None
        }
      }
    }

  def create(conn: Connection)(copyInfo: CopyInfo) {
    // if copyInfo.auditTableName exists, we want to use its tablespace.  Othewise we'll ask
    // postgresTablespaceSuffixFor to generate one.
    val ts: Option[String] = tablespaceOfTable(copyInfo.datasetInfo.auditTableName).getOrElse(None)
    using(conn.createStatement()) { stmt =>
      stmt.execute("CREATE TABLE " + copyInfo.dataTableName + " ()" + postgresTablespaceSuffixFor(ts))
    }
  }

  def computeStatistics(conn: Connection):PreloadStatistics = 0
  def updateStatistics(conn: Connection, rowsAdded: Long, rowsDeleted: Long, rowsChanged: Long, preload:PreloadStatistics) {}

  def insertBatch[T](conn: Connection)(t:Inserter => T) = (0, t(new InserterImpl))

  class InserterImpl extends Inserter {
    def insert(row: _root_.com.socrata.datacoordinator.Row[CV]) = {}
  }
}
