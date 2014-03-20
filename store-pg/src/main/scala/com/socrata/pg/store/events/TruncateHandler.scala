package com.socrata.pg.store.events

import com.socrata.pg.store.PGSecondaryUniverse
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.metadata.CopyInfo

case class TruncateHandler(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) {
  pgu.truncator.truncate(copyInfo, pgu.logger(copyInfo.datasetInfo, "not-used"))
}