package com.socrata.pg.server

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}

/**
 * Allow a pg secondary universe passed in so that tests can read what have just been written.
 * @param dsConfig
 * @param pgu
 */
class QueryServerTest(dsConfig: DataSourceConfig, pgu: PGSecondaryUniverse[SoQLType, SoQLValue]) extends QueryServer(dsConfig) {

  override protected def withPgu[T]()(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    withDb() { conn =>
      f(pgu)
    }
  }
}
