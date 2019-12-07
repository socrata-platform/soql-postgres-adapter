package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{Delete, Insert, Operation, Update}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.pg.error.RowSizeBufferSqlErrorResync
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

object RowDataUpdatedHandler {
  private val logger = Logger[RowDataUpdatedHandler]
}

case class RowDataUpdatedHandler(loader: SqlPrevettedLoader[SoQLType, SoQLValue],
                                 ops: Seq[Operation[SoQLValue]]) {
  import RowDataUpdatedHandler.logger

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
