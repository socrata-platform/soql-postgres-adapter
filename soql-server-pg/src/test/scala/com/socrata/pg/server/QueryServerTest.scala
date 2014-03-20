package com.socrata.pg.server

import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.pg.soql.CaseSensitive

/**
 * Allow a pg secondary universe passed in so that tests can read what have just been written.
 * @param dsInfo
 * @param pgu
 */
class QueryServerTest(dsInfo:DSInfo, pgu: PGSecondaryUniverse[SoQLType, SoQLValue]) extends QueryServer(dsInfo, CaseSensitive) {

  override protected def withPgu[T](dsInfo:DSInfo, truthStoreDatasetInfo:Option[DatasetInfo])(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    f(pgu)
  }
}
