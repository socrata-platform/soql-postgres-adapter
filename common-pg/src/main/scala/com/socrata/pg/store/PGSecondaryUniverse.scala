package com.socrata.pg.store

import java.sql.Connection
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.universe.sql.PostgresCommonSupport
import com.socrata.datacoordinator.truth.loader.{ReportWriter, Logger}
import com.socrata.datacoordinator.truth.metadata.{DatasetMapWriter, DatasetCopyContext}
import com.socrata.datacoordinator.util.{RowVersionProvider, RowIdProvider}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.RepBasedPostgresSchemaLoader
import org.joda.time.DateTime
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapWriter

/**
 *
 */
class PGSecondaryUniverse[SoQLType, SoQLValue](conn: Connection,
                                                commonSupport: PostgresCommonSupport[SoQLType, SoQLValue])
  extends Universe[SoQLType, SoQLValue]
  with Commitable
  with SchemaLoaderProvider
  with LoaderProvider
  with DatasetMapWriterProvider
{
  import commonSupport._
  private val txnStart = DateTime.now()

  def transactionStart = txnStart

  def commit() = {
    println("I am committing here")
    conn.commit
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

  lazy val datasetMapWriter: DatasetMapWriter[CT] =
    new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialCounterValue)
}


