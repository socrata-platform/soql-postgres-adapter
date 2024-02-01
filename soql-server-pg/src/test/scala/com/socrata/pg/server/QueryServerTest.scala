package com.socrata.pg.server

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.pg.soql.CaseSensitive

import scala.concurrent.duration.Duration

/**
 * Allow a pg secondary universe passed in so that tests can read what have just been written.
 * @param dsInfo
 * @param pgu
 */
class QueryServerTest(dsInfo:DSInfo, pgu: PGSecondaryUniverse[SoQLType, SoQLValue]) extends
  QueryServer(dsInfo, CaseSensitive, new analyzer2.ProcessQuery("test"), leadingSearch = true, Duration.Zero) {

  override protected def withPgu[T](dsInfo:DSInfo, truthStoreDatasetInfo:Option[DatasetInfo])(f: (PGSecondaryUniverse[SoQLType, SoQLValue]) => T): T = {
    f(pgu)
  }
}
