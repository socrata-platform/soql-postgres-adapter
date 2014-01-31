package com.socrata.pg.store

import java.sql.Connection
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.universe.sql.PostgresCommonSupport
import com.socrata.datacoordinator.truth.loader.{ReportWriter, Logger}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.{RowVersionProvider, RowIdProvider}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql.{PostgresRepBasedDataSqlizer, SqlPrevettedLoader, RepBasedPostgresSchemaLoader}
import org.joda.time.DateTime
import com.socrata.datacoordinator.truth.metadata.sql.{PostgresDatasetMapReader, PostgresDatasetMapWriter}
import com.socrata.datacoordinator.truth.sql.{PostgresDatabaseReader, PostgresDatabaseMutator, RepBasedSqlDatasetContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.truth.{LowLevelDatabaseReader, DatasetReader, DatasetMutator}

/**
 *
 */
class PGSecondaryUniverse[SoQLType, SoQLValue](conn: Connection,
                                                commonSupport: PostgresCommonSupport[SoQLType, SoQLValue])
  extends Universe[SoQLType, SoQLValue]
  with Commitable
  with SchemaLoaderProvider
  with PrevettedLoaderProvider
  with DatasetMapWriterProvider
  with DatasetMapReaderProvider
  with DatasetReaderProvider
{
  import commonSupport._
  private val txnStart = DateTime.now()

  def transactionStart = txnStart

  def commit() = {
    conn.commit
  }

  def rollback() = {
    conn.abort(commonSupport.executor)
  }

  def prevettedLoader(copyCtx: DatasetCopyContext[CT], logger: Logger[CT, CV]) =
    new SqlPrevettedLoader(conn, sqlizerFactory(copyCtx.copyInfo, datasetContextFactory(copyCtx.schema)), logger)

  def sqlizerFactory(copyInfo: CopyInfo, datasetContext: RepBasedSqlDatasetContext[CT, CV]) =
    new PostgresRepBasedDataSqlizer(copyInfo.dataTableName, datasetContext, copyInProvider)

  def datasetContextFactory(schema: ColumnIdMap[ColumnInfo[CT]]): RepBasedSqlDatasetContext[CT, CV] = {
    RepBasedSqlDatasetContext(
      typeContext,
      schema.mapValuesStrict(repFor),
      schema.values.find(_.isUserPrimaryKey).map(_.systemId),
      schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system primary key?")).systemId,
      schema.values.find(_.isVersion).getOrElse(sys.error("No version column?")).systemId,
      schema.keySet.filter { cid => isSystemColumn(schema(cid)) }
    )
  }

  def schemaLoader(logger: Logger[SoQLType, SoQLValue]) = new RepBasedPostgresSchemaLoader(conn, logger, repFor, tablespace)

  def loader(copyCtx: DatasetCopyContext[CT], rowIdProvider: RowIdProvider, rowVersionProvider: RowVersionProvider, logger: Logger[CT, CV], reportWriter: ReportWriter[CV], replaceUpdatedRows: Boolean) =
    managed(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx, replaceUpdatedRows), rowIdProvider, rowVersionProvider, logger, reportWriter, timingReport))

  lazy val datasetMapWriter: DatasetMapWriter[CT] =
    new PostgresDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialCounterValue)

  lazy val datasetMapReader: DatasetMapReader[CT] =
    new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport)

  lazy val datasetReader = DatasetReader(lowLevelDatabaseReader)

  lazy val lowLevelDatabaseReader = new PostgresDatabaseReader(conn, datasetMapReader, repFor)

  def openDatabase = lowLevelDatabaseReader.openDatabase _
}


