package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{Delete, Insert, Operation, Update}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.pg.error.RowSizeBufferSqlErrorResync
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.slf4j.Logging

case class RowDataUpdatedHandler(loader: SqlPrevettedLoader[SoQLType, SoQLValue], ops: Seq[Operation[SoQLValue]]) extends Logging {
  RowSizeBufferSqlErrorResync.guard(loader.conn) {
    ops.foreach {
      o =>
        logger.debug("Got row operation: {}", o)
        o match {
          case Insert(sid, row) => loader.insert(sid, row)
          case Update(sid, row) => loader.update(sid, None, row)
          case Delete(sid) => loader.delete(sid, None)
        }
    }
    loader.flush()
  }
}