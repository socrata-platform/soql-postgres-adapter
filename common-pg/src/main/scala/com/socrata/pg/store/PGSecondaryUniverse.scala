package com.socrata.pg.store

import java.sql.Connection
import com.socrata.datacoordinator.truth.universe.{LoaderProvider, SchemaLoaderProvider, Commitable, Universe}
import com.socrata.datacoordinator.truth.universe.sql.PostgresCommonSupport
import com.socrata.datacoordinator.truth.loader.{ReportWriter, Logger}
import com.socrata.datacoordinator.truth.metadata.DatasetCopyContext
import com.socrata.datacoordinator.util.{RowVersionProvider, RowIdProvider}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.RepBasedPostgresSchemaLoader
import org.joda.time.DateTime

/**
 *
 */
class PGSecondaryUniverse[SoQLType, SoQLValue](conn: Connection,
                                                commonSupport: PostgresCommonSupport[SoQLType, SoQLValue])
  extends Universe[SoQLType, SoQLValue]
  with Commitable
  with SchemaLoaderProvider
  with LoaderProvider
{
  import commonSupport._
  private val txnStart = DateTime.now()

  def transactionStart = txnStart

  def commit() = {
    println("I would commit here")
  }

  def rollback() = { println("I would rollback here")}

  def loader(copyCtx: DatasetCopyContext[SoQLType],
             rowIdProvider: RowIdProvider,
             rowVersionProvider: RowVersionProvider,
             logger: Logger[SoQLType, SoQLValue],
             reportWriter: ReportWriter[SoQLValue],
             replaceUpdatedRows: Boolean) = {
    managed(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx, replaceUpdatedRows), rowIdProvider, rowVersionProvider, logger, reportWriter, timingReport))
  }

  def schemaLoader(logger: Logger[SoQLType, SoQLValue]) = new RepBasedPostgresSchemaLoader(conn, logger, repFor, tablespace)
}


