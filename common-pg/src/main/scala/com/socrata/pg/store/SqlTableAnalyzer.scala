package com.socrata.pg.store

import java.sql.Connection
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.truth.metadata.CopyInfo

class SqlTableAnalyzer(conn: Connection) extends TableAnalyzer {
  override def analyze(copy: CopyInfo): Unit = {
    using(conn.createStatement()) { stmt =>
      stmt.execute("ANALYZE " + copy.dataTableName)
    }
  }
}
