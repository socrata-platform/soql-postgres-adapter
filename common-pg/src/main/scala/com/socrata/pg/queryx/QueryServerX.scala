package com.socrata.pg.queryx

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.common.DataSourceConfig
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.id.{ColumnId, RollupName, UserColumnId}
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetCopyContext, DatasetInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.server.util.{EntityTag, Precondition, StrongEntityTag}
import com.socrata.http.server.util.Precondition.{FailedBecauseMatch, FailedBecauseNoMatch, Passed}
import com.socrata.pg.SecondaryBase
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.soql._
import com.socrata.pg.store.{PGSecondaryRowReader, PGSecondaryUniverse, PostgresUniverseCommon, SchemaUtil, SqlUtils}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.stdlib.{Context => SoQLContext}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.SoQLID.ClearNumberRep
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.types.{SoQLID, SoQLType, SoQLValue, SoQLVersion}
import com.socrata.soql.{BinaryTree, Compound, Leaf, SoQLAnalysis, typed}
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{Connection, SQLException}
import scala.concurrent.duration.Duration

class QueryServerX(val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity, val leadingSearch: Boolean = true) extends SecondaryBase {

  private val logger = Logger[QueryServerX]

  val dsConfig: DataSourceConfig = null // scalastyle:ignore null // unused

  val postgresUniverseCommon = PostgresUniverseCommon

  def execQuery( // scalastyle:ignore method.length parameter.number cyclomatic.complexity
                 pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                 context: SoQLContext,
                 datasetInternalName: String,
                 datasetInfo: DatasetInfo,
                 analysis: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
                 rowCount: Boolean,
                 reqCopy: Option[String],
                 rollupName: Option[RollupName],
                 obfuscateId: Boolean,
                 precondition: Precondition,
                 ifModifiedSince: Option[DateTime],
                 givenLastModified: Option[DateTime],
                 etagInfo: Option[String],
                 queryTimeout: Option[Duration],
                 debug: Boolean,
                 explain: Boolean,
                 analyze: Boolean
               ): Unit /* QueryResult */  = {

    def runQuery(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                 context: SoQLContext,
                 latestCopy: CopyInfo,
                 analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
                 rowCount: Boolean,
                 queryTimeout: Option[Duration],
                 explain: Boolean,
                 analyze: Boolean): Unit /* QueryResult */ = {
      val cryptProvider = new CryptProvider(latestCopy.datasetInfo.obfuscationKey)

      val sqlCtx = Map[SqlizerContext, Any](
        SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
        else { new ClearNumberRep(cryptProvider) }),
        SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
        SqlizerContext.CaseSensitivity -> caseSensitivity,
        SqlizerContext.LeadingSearch -> leadingSearch
      )
      val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

      val lma = analyses.leftMost.leaf

      for(readCtx <- pgu.datasetReader.openDataset(latestCopy)) {
        val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
        val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
        val qrySchema = querySchema(pgu, analyses.outputSchema.leaf, latestCopy)
        val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor)
        val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
        val sqlReps = querier.getSqlReps(readCtx.copyInfo.dataTableName, systemToUserColumnMap)

        // TODO: rethink how reps should be constructed and passed into each chained soql.
        val typeReps = analyses.seq.flatMap { analysis =>
          val qrySchema = querySchema(pgu, analysis, latestCopy)
          qrySchema.map { case (columnId, columnInfo) =>
            columnInfo.typ -> pgu.commonSupport.repFor(columnInfo)
          }
        }.toMap

        // rollups will cause querier's dataTableName to be different than the normal dataset tablename
        val tableName = querier.sqlizer.dataTableName
        val copyInfo = readCtx.copyInfo
        val joinCopiesMap = getJoinCopies(pgu, analysis, reqCopy) ++ copyInfo.datasetInfo.resourceName.map(rn => Map(TableName(rn) -> copyInfo)).getOrElse(Map.empty)

        val sqlRepsWithJoin = joinCopiesMap.foldLeft(sqlReps) { (acc, joinCopy) =>
          val (tableName, copyInfo) = joinCopy
          acc ++ getJoinReps(pgu, copyInfo, tableName)
        }

        val thisTableNameMap = lma.from match {
          case Some(tn@(TableName(name, _))) if name == TableName.This =>
            Map(tn.copy(alias = None) -> tableName)
          case _ =>
            Map.empty
        }

        if (explain) {
          val explain = querier.queryExplain(
            context,
            analyses,
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) + (TableName.PrimaryTable -> tableName)
              Sqlizer.sql((as, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            queryTimeout,
            debug,
            analyze)

          ///// InfoQueryResult(latestCopy.dataVersion, explain)
        } else {
          val results = querier.query(
            context,
            analyses,
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) ++ thisTableNameMap +
                (TableName.PrimaryTable -> tableName) + (TableName(tableName) -> tableName)
              Sqlizer.sql((as, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            (as: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]) => {
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) ++ thisTableNameMap + (TableName.PrimaryTable -> tableName) + (TableName(tableName) -> tableName)
              BinarySoQLAnalysisSqlizer.rowCountSql((as, tableNameMap, sqlReps.values.toSeq))(
                sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            rowCount,
            qryReps,
            queryTimeout,
            debug)

          ///// RowsQueryResult(qrySchema, latestCopy.dataVersion, results)
        }
      }
    }

    val copy = getCopy(pgu, datasetInfo, reqCopy)
    if(debug) {
      logger.info(s"etagInfo: ${etagInfo.map(_.getBytes(StandardCharsets.ISO_8859_1).mkString(","))}")
      logger.info(s"datasetInternalName: $datasetInternalName")
      logger.info(s"copy: $copy")
    }
    val etag = etagFromCopy(datasetInternalName, copy, etagInfo, debug)
    if(debug) logger.info(s"Generated etag: ${etag.asBytes.mkString(",")}")
    val lastModified = givenLastModified match {
      case None => copy.lastModified
      case Some(d) => if (d.isAfter(copy.lastModified)) d else copy.lastModified
    }

    // Conditional GET handling
//    precondition.check(Some(etag), sideEffectFree = true) match {
//      case Passed =>
//        ifModifiedSince match {
//          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
//            && precondition == NoPrecondition =>
//            NotModified(Seq(etag))
//          case Some(_) | None =>
//            try {
//              runQuery(pgu, context, copy, analysis, rowCount, queryTimeout, explain, analyze) match {
//                case RowsQueryResult(qrySchema, version, results) =>
//                  Success (qrySchema, copy.copyNumber, version, results, etag, lastModified)
//                case InfoQueryResult(dataVersion, explainInfo) =>
//                  InfoSuccess(copy.copyNumber, dataVersion, explainInfo)
//              }
//            } catch {
//              case ex: SQLException =>
//                QueryRuntimeError(ex) match {
//                  case Some(error) => error
//                  case None => throw ex
//                }
//              case ex: SqlizeError =>
//                QueryError(ex.getMessage)
//            }
//        }
//      case FailedBecauseMatch(etags) =>
//        NotModified(etags)
//      case FailedBecauseNoMatch =>
//        PreconditionFailed
//    }
  }

  private def readerWithQuery[SoQLType, SoQLValue](conn: Connection,
                                                   pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                                   copyCtx: DatasetCopyContext[SoQLType],
                                                   schema: ColumnIdMap[ColumnInfo[SoQLType]],
                                                   rollupName: Option[RollupName]):
  PGSecondaryRowReader[SoQLType, SoQLValue] with RowReaderQuerier[SoQLType, SoQLValue] = {

    val tableName = rollupName match {
      case Some(r) =>
        val rollupInfo = pgu.datasetMapReader.rollup(copyCtx.copyInfo, r).getOrElse {
          throw new RuntimeException(s"Rollup ${rollupName} not found for copy ${copyCtx.copyInfo} ")
        }
        rollupInfo.tableName
      case None =>
        copyCtx.copyInfo.dataTableName
    }

    new PGSecondaryRowReader[SoQLType, SoQLValue] (
      conn,
      new PostgresRepBasedDataSqlizer(
        tableName,
        pgu.datasetContextFactory(schema),
        pgu.commonSupport.copyInProvider
      ) with DataSqlizerQuerier[SoQLType, SoQLValue],
      pgu.commonSupport.timingReport
    ) with RowReaderQuerier[SoQLType, SoQLValue]
  }

  private def getJoinCopies(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
                            reqCopy: Option[String]): Map[TableName, CopyInfo] = {
    val joins = typed.Join.expandJoins(analyses.seq)
    val froms = analyses.seq.flatMap(x => x.from.toSeq)
    val joinTables = joins.flatMap(x => x.from.fromTables) ++ froms
    val joinTableMap = joinTables.flatMap { resourceName =>
      getCopy(pgu, new ResourceName(resourceName.name), reqCopy).map(copy => (resourceName, copy))
    }.toMap

    val allMap = joinTableMap
    val allMapNoAliases = allMap.map {
      case (tableName, copy) =>
        (tableName.copy(alias = None), copy)
    }

    allMapNoAliases
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], ds: String, reqCopy: Option[String])
  : Option[CopyInfo] = {
    for {
      datasetId <- pgu.datasetMapReader.datasetIdForInternalName(ds, checkDisabled = true)
      datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      getCopy(pgu, datasetInfo, reqCopy)
    }
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                      datasetInfo: DatasetInfo,
                      reqCopy: Option[String]): CopyInfo = {
    val intRx = "^[0-9]+$".r
    val rd = pgu.datasetMapReader
    reqCopy match {
      case Some("latest") =>
        rd.latest(datasetInfo)
      case Some("published") | None =>
        rd.published(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some("unpublished") | None =>
        rd.unpublished(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some(intRx(num)) =>
        rd.copyNumber(datasetInfo, num.toLong).getOrElse(rd.latest(datasetInfo))
      case Some(unknown) =>
        throw new IllegalArgumentException(s"invalid copy value $unknown")
    }
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], resourceName: ResourceName, reqCopy: Option[String]): Option[CopyInfo] = {
    for {
      datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(resourceName)
    } yield {
      getCopy(pgu, datasetInfo, reqCopy)
    }
  }

  private def getDatasetTablenameMap(copies: Map[TableName, CopyInfo]): Map[TableName, String] = {
    copies.mapValues { copy =>
      copy.dataTableName
    }
  }

  private def getJoinReps(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                          copy: CopyInfo,
                          tableName: TableName)
  : Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]] = {
    for(readCtx <- pgu.datasetReader.openDataset(copy)) {
      val schema = readCtx.schema
      val columnIdUserColumnIdMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val columnIdReps = schema.mapValuesStrict(pgu.commonSupport.repFor)
      columnIdReps.keys.map { columnId =>
        val userColumnId: UserColumnId = columnIdUserColumnIdMap(columnId)
        val qualifier = tableName.alias.orElse(copy.datasetInfo.resourceName)
        val qualifiedUserColumnId = new QualifiedUserColumnId(qualifier, userColumnId)
        qualifiedUserColumnId -> columnIdReps(columnId)
      }.toMap
    }
  }

  private def querySchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                          analysis: SoQLAnalysis[UserColumnId, SoQLType],
                          latest: CopyInfo):
  OrderedMap[ColumnId, ColumnInfo[pgu.CT]] = {

    analysis.selection.foldLeft(OrderedMap.empty[ColumnId, ColumnInfo[pgu.CT]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new ColumnId(map.size + 1)
          val cinfo = new ColumnInfo[pgu.CT](
            latest,
            cid,
            new UserColumnId(columnName.name),
            None, // field name
            coreExpr.typ,
            columnName.name,
            coreExpr.typ == SoQLID,
            false, // isUserKey
            coreExpr.typ == SoQLVersion,
            None, // computation strategy we aren't actually storing...
            Seq.empty
          )(SoQLTypeContext.typeNamespace, null) // scalastyle:ignore null
          map + (cid -> cinfo)
      }
    }
  }

  def etagFromCopy(datasetInternalName: String, copy: CopyInfo, etagInfo: Option[String], debug: Boolean = false): EntityTag = {
    // ETag is a hash based on datasetInternalName_copyNumber_version
    // Upstream components may pass through etag headers to and from query server but generate different queries.
    // For example, "select *" may become "select `a`" or "select `a`, `newly_unhidden_colujmn`"
    // Including query string using etagInfo in hash generation makes etags more robust.
    val etagInfoDigest = etagInfo.map { x =>
      val md = MessageDigest.getInstance("SHA1")
      md.update(x.getBytes(StandardCharsets.UTF_8))
      Base64.encodeBase64URLSafeString(md.digest())
    }.getOrElse("")
    val etagContents = s"${datasetInternalName}_${copy.copyNumber}_${copy.dataVersion}$etagInfoDigest"
    if(debug) logger.info(s"etagContents: $etagContents")
    StrongEntityTag(etagContents.getBytes(StandardCharsets.UTF_8))
  }

  def createEtagFromAnalysis(analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]], fromTable: String, contextVars: SoQLContext): String = {
    val suffix =
      if(contextVars.nonEmpty) {
        JsonUtil.renderJson(contextVars.canonicalized, pretty = false)
      } else {
        ""
      }

    // Strictly speaking this table name is incorrect, but it's _consistently_ incorrect
    val etag = analyses match {
      case Compound(op, l, r) =>
        createEtagFromAnalysis(l, fromTable, SoQLContext.empty) + op + createEtagFromAnalysis(r, fromTable, SoQLContext.empty)
      case Leaf(analysis) =>
        if (analysis.from.isDefined) analysis.toString()
        else analysis.toStringWithFrom(TableName(fromTable))
    }
    etag + suffix
  }
}
