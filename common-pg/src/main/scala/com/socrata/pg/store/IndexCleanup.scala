package com.socrata.pg.store

import javax.sql.DataSource

trait IndexCleanup {
  def cleanupPendingDrops(dataSource: DataSource, host: String): Boolean
}
