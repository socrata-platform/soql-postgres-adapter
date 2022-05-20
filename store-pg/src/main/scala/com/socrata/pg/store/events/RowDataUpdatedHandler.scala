package com.socrata.pg.store.events

import com.socrata.datacoordinator.secondary.{Delete, Insert, Operation, Update}
import com.socrata.datacoordinator.truth.loader.sql.SqlPrevettedLoader
import com.socrata.pg.error.RowSizeBufferSqlErrorResync
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

sealed abstract class RowDataUpdatedHandler

object RowDataUpdatedHandler {
  private val logger = Logger[RowDataUpdatedHandler]

  // Log progress at most every ten minutes
  private val IntervalBetweenTicksNS = 10*60*1e9.toLong

  case class Progress(expectedOps: Long = 0, performedOps: Long = 0, lastTickedAt: Long = 0)

  def apply(loader: SqlPrevettedLoader[SoQLType, SoQLValue],
            ops: Seq[Operation[SoQLValue]],
            progress: Progress): Progress =
  {
    val tickEvery = progress.expectedOps / 100

    var performedOps = progress.performedOps
    var lastTickedAt = if(progress.performedOps == 0) System.nanoTime() else progress.lastTickedAt

    RowSizeBufferSqlErrorResync.guard(loader.conn) {
      ops.foreach { o =>
        logger.debug("Got row operation: {}", o)
        o match {
          case Insert(sid, row) =>
            loader.insert(sid, row)
          case Update(sid, row) =>
            loader.update(sid, None, row)
          case Delete(sid) =>
            loader.delete(sid, None)
        }

        performedOps += 1
        val now = System.nanoTime()
        if(now - lastTickedAt > IntervalBetweenTicksNS) {
          logger.info("Upsert progress: {}/{} ({}%) operations performed", performedOps, progress.expectedOps, "%.2f".format(100.0 * performedOps.toDouble / progress.expectedOps))
          lastTickedAt = now
        }
      }
      loader.flush()
    }

    progress.copy(performedOps = performedOps, lastTickedAt = lastTickedAt)
  }
}
