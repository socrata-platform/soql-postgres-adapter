package com.socrata.pg.store

import java.io.{File, OutputStream}
import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLRowLogCodec, SoQLTypeContext}
import com.socrata.datacoordinator.id.{DatasetId, RowId, RowVersion, UserColumnId}
import com.socrata.datacoordinator.truth.SimpleRowLogCodec
import com.socrata.datacoordinator.truth.json.JsonColumnRep
import com.socrata.datacoordinator.truth.loader.RowPreparer
import com.socrata.datacoordinator.truth.metadata.{AbstractColumnInfoLike, ColumnInfo, DatasetCopyContext, DatasetInfo}
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits
import com.socrata.datacoordinator.truth.universe.sql.{PostgresCommonSupport, PostgresCopyIn}
import com.socrata.datacoordinator.util.collection.{ColumnIdSet, MutableColumnIdSet, UserColumnIdMap}
import com.socrata.datacoordinator.util.{DebugLoggedTimingReport, StackedTimingReport}
import com.socrata.datacoordinator.{MutableRow, Row}
import com.socrata.pg.soql.SqlColIdx
import com.socrata.pg.store.index.{FullTextSearch, IndexSupport, SoQLIndexableRep}
import com.socrata.soql.SoQLAnalyzer
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, FiniteDuration}

object StandardDatasetMapLimits extends DatasetMapLimits

object SoQLSystemColumns {
  sc =>
  val id = new UserColumnId(":id")
  val createdAt = new UserColumnId(":created_at")
  val updatedAt = new UserColumnId(":updated_at")
  val version = new UserColumnId(":version")

  val schemaFragment = UserColumnIdMap(
    id -> SoQLID,
    version -> SoQLVersion,
    createdAt -> SoQLFixedTimestamp,
    updatedAt -> SoQLFixedTimestamp
  )

  val allSystemColumnIds = schemaFragment.keySet

  def isSystemColumnId(name: UserColumnId): Boolean =
    name.underlying.startsWith(":") && !name.underlying.startsWith(":@")
}

class PostgresUniverseCommon(val tablespace: String => Option[String],
                             val copyInProvider: (Connection, String, OutputStream => Unit) => Long)
      extends PostgresCommonSupport[SoQLType, SoQLValue]
      with IndexSupport[SoQLType, SoQLValue]
      with FullTextSearch[SoQLType] {
  val typeContext = SoQLTypeContext

  val repForIndex: ColumnInfo[SoQLType] => SqlColIdx = SoQLIndexableRep.sqlRep

  val repFor = repForIndex

  protected val SearchableTypes: Set[SoQLType] = Set(SoQLText, SoQLObject, SoQLArray)
  protected val SearchableNumericTypes: Set[SoQLType] = Set(SoQLNumber)

  def tmpDir: File = File.createTempFile("pg-store", "pg").getParentFile
  // unused
  def logTableCleanupDeleteEvery: FiniteDuration = ??? // scalastyle:ignore
  def logTableCleanupDeleteOlderThan: FiniteDuration = ??? // scalastyle:ignore

  val SystemColumns = SoQLSystemColumns

  val newRowCodec = newRowLogCodec _

  def newRowLogCodec(): SimpleRowLogCodec[SoQLValue] = SoQLRowLogCodec
  private val internalNamePrefix = "pg."

  def isSystemColumn(ci: AbstractColumnInfoLike): Boolean =
    isSystemColumnId(ci.userColumnId)

  def isSystemColumnId(name: UserColumnId): Boolean =
    SoQLSystemColumns.isSystemColumnId(name)

  val datasetIdFormatter = internalNameFromDatasetId _

  def internalNameFromDatasetId(datasetId: DatasetId): String =
    internalNamePrefix + datasetId.underlying

  val writeLockTimeout = Duration.create(1, TimeUnit.MINUTES)

  def idObfuscationContextFor(cryptProvider: CryptProvider, obfuscateId: Boolean): SoQLID.StringRep = {
    if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
    else { new SoQLID.ClearNumberRep(cryptProvider) }
  }

  def versionObfuscationContextFor(cryptProvider: CryptProvider): SoQLVersion.StringRep =
    new SoQLVersion.StringRep(cryptProvider)

  def jsonReps(datasetInfo: DatasetInfo, obfuscateId: Boolean): SoQLType => JsonColumnRep[SoQLType,SoQLValue] = {
    val cp = new CryptProvider(datasetInfo.obfuscationKey)
    SoQLRep.jsonRep(idObfuscationContextFor(cp, obfuscateId), versionObfuscationContextFor(cp))
  }

  def rowPreparer(transactionStart: DateTime, // scalastyle:ignore method.length
                  ctx: DatasetCopyContext[SoQLType],
                  replaceUpdatedRows: Boolean): RowPreparer[SoQLValue] =
    new RowPreparer[SoQLValue] {
      val schema = ctx.schemaByUserColumnId
      lazy val jsonRepFor = jsonReps(ctx.datasetInfo, true)

      def findCol(name: UserColumnId) =
        schema.getOrElse(name, sys.error(s"No $name column?")).systemId
      val idColumn = findCol(SystemColumns.id)
      val createdAtColumn = findCol(SystemColumns.createdAt)
      val updatedAtColumn = findCol(SystemColumns.updatedAt)
      val versionColumn = findCol(SystemColumns.version)

      val columnsRequiredForDelete = ColumnIdSet(versionColumn)

      val primaryKeyColumn = ctx.pkCol_!

      assert(ctx.schema(versionColumn).typeName == typeContext.typeNamespace.nameForType(SoQLVersion))

      val allSystemColumns = locally {
        val result = MutableColumnIdSet()
        for { c <- SystemColumns.allSystemColumnIds } {
          result += findCol(c)
        }
        result.freeze()
      }

      def prepareForInsert(row: Row[SoQLValue], sid: RowId, version: RowVersion) = {
        val tmp = new MutableRow(row)
        tmp(idColumn) = SoQLID(sid.underlying)
        tmp(createdAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp(versionColumn) = SoQLVersion(version.underlying)
        tmp.freeze()
      }

      def baseRow(oldRow: Row[SoQLValue]): MutableRow[SoQLValue] =
        if (replaceUpdatedRows) {
          val blank = new MutableRow[SoQLValue]
          for { cid <- allSystemColumns.iterator } {
            if (oldRow.contains(cid)) blank(cid) = oldRow(cid)
          }
          blank
        } else {
          new MutableRow[SoQLValue](oldRow)
        }

      def prepareForUpdate(row: Row[SoQLValue], oldRow: Row[SoQLValue], newVersion: RowVersion): Row[SoQLValue] = {
        val tmp = baseRow(oldRow)
        val rowIt = row.iterator
        while (rowIt.hasNext) {
          rowIt.advance()
          if (!allSystemColumns(rowIt.key)) tmp(rowIt.key) = rowIt.value
        }
        tmp(updatedAtColumn) = SoQLFixedTimestamp(transactionStart)
        tmp(versionColumn) = SoQLVersion(newVersion.underlying)
        tmp.freeze()
      }
    }

  def generateObfuscationKey(): Array[Byte] = CryptProvider.generateKey()

  def soqlAnalyzer: SoQLAnalyzer[SoQLType] = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  val executor: ExecutorService = Executors.newCachedThreadPool()
  val obfuscationKeyGenerator: () => Array[Byte] = generateObfuscationKey
  val initialCounterValue: Long = 0L
  val timingReport = new DebugLoggedTimingReport(org.slf4j.LoggerFactory.getLogger("timing-report")) with StackedTimingReport
}

/**
 * This instance of PostgresUniverseCommon ignores tablespace configuration.
 * It also ignores PG connection specific copyIn that is needed by secondary watcher.
 * Look for C3P0WrappedPostgresCopyIn and DatasourceFromConfig for details.
 */
object PostgresUniverseCommon extends PostgresUniverseCommon((s: String) => None, PostgresCopyIn)
