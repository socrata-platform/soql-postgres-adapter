package com.socrata.pg.store

import java.sql.Connection
import com.socrata.datacoordinator.truth.universe._
import com.socrata.datacoordinator.truth.universe.sql.{SqlTableCleanup, PostgresCommonSupport}
import com.socrata.datacoordinator.truth.loader.{ReportWriter, Logger}
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.NullCache
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.truth.loader.sql._
import org.joda.time.DateTime
import com.socrata.datacoordinator.truth.metadata.sql.PostgresDatasetMapReader
import com.socrata.datacoordinator.truth.sql.{PostgresDatabaseReader, RepBasedSqlDatasetContext}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.DatasetReader
import com.socrata.datacoordinator.truth.metadata.SchemaFinder
import com.socrata.pg.store.index.{FullTextSearch, IndexSupport}
import com.socrata.datacoordinator.secondary
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.metadata.DatasetInfo
import com.socrata.datacoordinator.truth.metadata.CopyInfo
import com.socrata.datacoordinator.id.DatasetId
import com.socrata.pg.error.RowSizeBufferSqlErrorContinue


/**
 *
 */
class PGSecondaryUniverse[SoQLType, SoQLValue](
  val conn: Connection,
  val commonSupport: PostgresCommonSupport[SoQLType, SoQLValue] with IndexSupport[SoQLType, SoQLValue] with FullTextSearch[SoQLType],
  val truthStoreDatasetInfo:Option[secondary.DatasetInfo] = None)
  extends Universe[SoQLType, SoQLValue]
  with Commitable
  with SchemaLoaderProvider
  with SchemaFinderProvider
  with DatasetMapWriterProvider
  with DatasetMapReaderProvider
  with DatasetReaderProvider
  with LoggerProvider
  with RowReaderProvider[SoQLType, SoQLValue]
  with TruncatorProvider
  with DatasetDropperProvider
  with TableCleanupProvider
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

  def prevettedLoader(copyCtx: DatasetCopyContext[SoQLType], logger: Logger[SoQLType, SoQLValue]) =
    new SqlPrevettedLoader(conn, sqlizerFactory(copyCtx.copyInfo, datasetContextFactory(copyCtx.schema)), logger)


  def sqlizerFactory(copyInfo: CopyInfo, datasetContext: RepBasedSqlDatasetContext[SoQLType, SoQLValue]) =
    new PostgresRepBasedDataSqlizer(copyInfo.dataTableName, datasetContext, copyInProvider)

  def datasetContextFactory(schema: ColumnIdMap[ColumnInfo[SoQLType]]): RepBasedSqlDatasetContext[SoQLType, SoQLValue] = {
    RepBasedSqlDatasetContext(
      typeContext,
      schema.mapValuesStrict(repFor),
      schema.values.find(_.isUserPrimaryKey).map(_.systemId),
      schema.values.find(_.isSystemPrimaryKey).getOrElse(sys.error("No system primary key?")).systemId,
      schema.values.find(_.isVersion).getOrElse(sys.error("No version column?")).systemId,
      schema.keySet.filter { cid => isSystemColumn(schema(cid)) }
    )
  }

  def schemaLoader(logger: Logger[SoQLType, SoQLValue]) =
    new SecondarySchemaLoader(conn, logger, repForIndex, tablespace, commonSupport, RowSizeBufferSqlErrorContinue)

  def obfuscationKeyGenerator() = truthStoreDatasetInfo match {
    case Some(dsi) => dsi.obfuscationKey
    case None => CryptProvider.generateKey()
  }

  //def loader(copyCtx: DatasetCopyContext[SoQLType], rowIdProvider: RowIdProvider, rowVersionProvider: RowVersionProvider, logger: Logger[SoQLType, SoQLValue], reportWriter: ReportWriter[SoQLValue], replaceUpdatedRows: Boolean) =
  //  managed(loaderProvider(conn, copyCtx, rowPreparer(transactionStart, copyCtx, replaceUpdatedRows), rowIdProvider, rowVersionProvider, logger, reportWriter, timingReport))

  // TODO: is it legit to expose the BackupDatasetMap (writer) here?  Need it to set ids...
  lazy val datasetMapWriter: PGSecondaryDatasetMapWriter[SoQLType]  =
    new PGSecondaryDatasetMapWriter(conn, typeContext.typeNamespace, timingReport, obfuscationKeyGenerator, initialCounterValue)

  lazy val datasetMapReader: DatasetMapReader[SoQLType] =
    new PostgresDatasetMapReader(conn, typeContext.typeNamespace, timingReport)

  lazy val secondaryDatasetMapReader = new PGSecondaryDatasetMapReader(conn)

  lazy val secondaryDatasetMapWriter = datasetMapWriter // TODO: remove this alias

  lazy val datasetReader = DatasetReader(lowLevelDatabaseReader)

  lazy val lowLevelDatabaseReader = new PostgresDatabaseReader(conn, datasetMapReader, repFor)

  def openDatabase = lowLevelDatabaseReader.openDatabase _

  def logger(datasetInfo: DatasetInfo, user: String) = new PGSecondaryLogger[SoQLType, SoQLValue]()

  def reader(copyCtx: DatasetCopyContext[SoQLType]) = new PGSecondaryRowReader[SoQLType, SoQLValue](conn, sqlizerFactory(copyCtx.copyInfo, datasetContextFactory(copyCtx.schema)), timingReport)

  def schemaFinder: SchemaFinder[PGSecondaryUniverse[SoQLType, SoQLValue]#CT] =
    // Skip schema cache for query coordinator already provides caching.
    new com.socrata.datacoordinator.service.SchemaFinder(commonSupport.typeContext.typeNamespace.userTypeForType _, NullCache)

  lazy val truncator = new SqlTruncator(conn)

  lazy val datasetDropper =
    new SqlDatasetDropper(conn, writeLockTimeout, datasetMapWriter) {
      override def updateSecondaryAndBackupInfo(datasetId: DatasetId, fakeVersion: Long) {
        // Does not apply to secondary.  Suppress what data coordinator implementation does.
      }
    }

  lazy val tableCleanup: TableCleanup =
    new SqlTableCleanup(conn, 0)
}
