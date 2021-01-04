package com.socrata.pg.server

import java.io.{ByteArrayInputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.Connection
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import javax.servlet.http.HttpServletResponse
import com.socrata.pg.BuildInfo
import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, JsonUtil}
import com.rojoma.simplearm.v2._
import com.socrata.NonEmptySeq
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.common.DataSourceFromConfig.DSInfo
import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.common.{DataSourceConfig, DataSourceFromConfig}
import com.socrata.datacoordinator.id._
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server._
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.implicits._
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.util.Precondition._
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.{EntityTag, NoPrecondition, Precondition, StrongEntityTag}
import com.socrata.pg.SecondaryBase
import com.socrata.pg.query.{DataSqlizerQuerier, RowCount, RowReaderQuerier}
import com.socrata.pg.server.config.{DynamicPortMap, QueryServerConfig}
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.soql._
import com.socrata.pg.store._
import com.socrata.soql.{BinaryTree, SoQLAnalysis, typed}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.SoQLID.ClearNumberRep
import com.socrata.soql.types.{SoQLID, SoQLType, SoQLValue, SoQLVersion}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.server.CJSONWriter.utf8EncodingName
import com.socrata.thirdparty.metrics.{MetricsReporter, SocrataHttpSupport}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.commons.codec.binary.Base64
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import org.apache.log4j.PropertyConfigurator
import org.joda.time.DateTime
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.existentials

class QueryServer(val dsInfo: DSInfo, val caseSensitivity: CaseSensitivity, val leadingSearch: Boolean = true) extends SecondaryBase {
  import QueryServer._ // scalastyle:ignore import.grouping

  val dsConfig: DataSourceConfig = null // scalastyle:ignore null // unused

  val postgresUniverseCommon = PostgresUniverseCommon

  private val JsonContentType = "application/json; charset=utf-8"

  private val routerSet = locally {
    Routes(
      Route("/schema", SchemaResource),
      Route("/rollups", RollupResource),
      Route("/query", QueryResource),
      Route("/version", VersionResource),
      Route("/info", InfoResource)
    )
  }

  private def route(req: HttpRequest): HttpResponse = {
    routerSet(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound
    }
  }

  object VersionResource extends SimpleResource {
    val response = OK ~> Content("application/json", BuildInfo.toJson)

    override val get = { _: HttpRequest => response }
  }

  object SchemaResource extends SimpleResource {
    override val get = schema _
  }

  def schema(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val copy = Option(servReq.getParameter("copy"))
    val withFieldName = Option(servReq.getParameter("fieldName")).map(java.lang.Boolean.parseBoolean(_)).getOrElse(false)

    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      // get schema by dataset id or resource name
      val copyInfoOpt = Option(servReq.getParameter("ds")) match {
        case Some(ds) =>
          getCopy(pgu, ds, copy)
        case None => Option(servReq.getParameter("rn")) match {
          case Some(rn) =>
            getCopy(pgu, new ResourceName(rn), copy)
          case None =>
            None
        }
      }

      copyInfoOpt match {
        case Some(copyInfo) =>
          if (withFieldName) {
            import com.socrata.pg.ExtendedSchema._
            val schema =  getSchemaWithFieldName(pgu, copyInfo)
            OK ~>
              copyInfoHeaderForSchema(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified) ~>
              Write(JsonContentType)(JsonUtil.writeJson(_, schema, buffer = true))
          } else {
            import com.socrata.pg.Schema._
            val schema =  getSchema(pgu, copyInfo)
            OK ~>
              copyInfoHeaderForSchema(copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lastModified) ~>
              Write(JsonContentType)(JsonUtil.writeJson(_, schema, buffer = true))
          }
        case None => NotFound
      }
    }
  }

  object RollupResource extends SimpleResource {
    override val get = rollups _
  }

  def rollups(req: HttpRequest): HttpResponse = {
    val servReq = req.servletRequest
    val ds = servReq.getParameter("ds")
    val copy = Option(servReq.getParameter("copy"))
    val includeUnmaterialized = java.lang.Boolean.parseBoolean(servReq.getParameter("include_unmaterialized"))
    getRollups(ds, copy, includeUnmaterialized) match {
      case Some(rollups) =>
        OK ~> Write(JsonContentType)(JsonUtil.writeJson(_, rollups.map(r => r.unanchored).toSeq, buffer = true))
      case None =>
        NotFound
    }
  }

  object QueryResource extends SimpleResource {
    override val get = query(false) _
    override val post = query(false) _
  }

  object InfoResource extends SimpleResource {
    override val post = query(true) _
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

  def createEtagFromAnalysis(analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]], fromTable: String): String = {
    analyses.map{
      analysis: SoQLAnalysis[UserColumnId, SoQLType] =>
        // Strictly speaking this table name is incorrect, but it's _consistently_ incorrect
        analysis.toStringWithFrom(TableName(fromTable))
    }.toString
  }

  /** maps dataset name to count of current requests.  Requires synchronization to access. */
  private val currentRequests = scala.collection.mutable.HashMap[String,Integer]().withDefaultValue(0)

  /**
    * Limits the maximum concurrent requests for the given dataset.  Requests beyond the limit will return
    * a HTTP 429 Too Many Requests response.
    */
  private def withRequestLimit(datasetId: String, resp: HttpServletResponse, f: HttpServletResponse => Unit) = {
    val tooManyAlready = currentRequests.synchronized {
      val currentRequestsForDataset = currentRequests(datasetId)
      logger.trace(s"Currently processing $currentRequestsForDataset for dataset $datasetId, max allowed is $QueryServer.config.maxConcurrentRequestsPerDataset")
      if (currentRequestsForDataset >= QueryServer.config.maxConcurrentRequestsPerDataset) {
        true
      } else {
        currentRequests(datasetId) = currentRequestsForDataset + 1
        false
      }
    }

    if (tooManyAlready) {
      logger.warn(s"Rejecting request for dataset $datasetId, already processing limit of $QueryServer.config.maxConcurrentRequestsPerDataset concurrent requests per dataset")
      TooManyRequests(resp)
    } else {
      try {
        f(resp)
      } finally {
        currentRequests.synchronized {
          val currentRequestsForDataset = currentRequests(datasetId)
          if (currentRequestsForDataset <= 1) {
            currentRequests.remove(datasetId)
          } else {
            currentRequests(datasetId) = currentRequestsForDataset - 1
          }
        }
      }
    }
  }

  private def query(explain: Boolean)(req: HttpRequest)(resp: HttpServletResponse): Unit =  {
    val servReq = req.servletRequest
    val datasetId = servReq.getParameter("dataset")
    val analysisParam = servReq.getParameter("query")
    val analysisStream = new ByteArrayInputStream(analysisParam.getBytes(StandardCharsets.ISO_8859_1))
    val analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = SoQLAnalyzerHelper.deserialize(analysisStream)
    val reqRowCount = Option(servReq.getParameter("rowCount")).map(_ == "approximate").getOrElse(false)
    val copy = Option(servReq.getParameter("copy"))
    val rollupName = Option(servReq.getParameter("rollupName")).map(new RollupName(_))
    val obfuscateId = !Option(servReq.getParameter("obfuscateId")).exists(_ == "false")
    val timeoutMs = Option(servReq.getParameter("queryTimeoutSeconds")).map(_.toDouble * 1000).map(_.toLong)
    val queryTimeout = timeoutMs.map(new FiniteDuration(_, TimeUnit.MILLISECONDS))
    val debug = Option(servReq.getParameter("X-Socrata-Debug")).isDefined
    val analyze = Option(servReq.getHeader("X-Socrata-Analyze")).exists(_ == "true") && explain

    withRequestLimit(datasetId, resp,
      streamQueryResults(
        analyses = analyses,
        datasetId = datasetId,
        reqRowCount = reqRowCount,
        copy = copy,
        rollupName = rollupName,
        obfuscateId = obfuscateId,
        precondition = req.precondition,
        ifModifiedSince = req.dateTimeHeader("If-Modified-Since"),
        etagInfo = Option(createEtagFromAnalysis(analyses, datasetId)),
        queryTimeout = queryTimeout,
        debug = debug,
        explain = explain,
        analyze = analyze))
  }

  def streamQueryResults( // scalastyle:ignore parameter.number
    analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]],
    datasetId: String,
    reqRowCount: Boolean,
    copy: Option[String],
    rollupName: Option[RollupName],
    obfuscateId: Boolean,
    precondition: Precondition,
    ifModifiedSince: Option[DateTime],
    etagInfo: Option[String],
    queryTimeout: Option[Duration],
    debug: Boolean,
    explain: Boolean,
    analyze: Boolean
  ) (resp:HttpServletResponse): Unit = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      pgu.secondaryDatasetMapReader.datasetIdForInternalName(datasetId, checkDisabled = true) match {
        case None =>
          logger.info(s"Tried to perform query on dataset $datasetId")
          NotFound(resp)
        case Some(dsId) =>
          pgu.datasetMapReader.datasetInfo(dsId) match {
            case None =>
              logger.info(s"Tried to perform query on dataset $datasetId")
              NotFound(resp)
            case Some(datasetInfo) =>
              def notModified(etags: Seq[EntityTag]) = responses.NotModified ~> ETags(etags)
              def requestTimeout(timeout: Option[Duration]) = responses.RequestTimeout ~> Json(Map("timeout" -> timeout.toString))
              execQuery(
                pgu = pgu,
                datasetInternalName = datasetId,
                datasetInfo = datasetInfo,
                analysis = analyses,
                rowCount = reqRowCount,
                reqCopy = copy,
                rollupName = rollupName,
                obfuscateId = obfuscateId,
                precondition = precondition,
                ifModifiedSince = ifModifiedSince,
                etagInfo = etagInfo,
                queryTimeout = queryTimeout,
                debug = debug,
                explain = explain,
                analyze = analyze) match {
                case NotModified(etags) => notModified(etags)(resp)
                case PreconditionFailed => responses.PreconditionFailed(resp)
                case RequestTimedOut(timeout) => requestTimeout(timeout)(resp)
                case Success(qrySchema, copyNumber, dataVersion, results, etag, lastModified) =>
                  // Very weird separation of concerns between execQuery and streaming. Most likely we will
                  // want yet-another-refactoring where much of execQuery is lifted out into this function.
                  // This will significantly change the tests; however.
                  if(debug) logger.info(s"Returning etag: ${etag.asBytes.mkString(",")}")
                  ETag(etag)(resp)
                  copyInfoHeaderForRows(copyNumber, dataVersion, lastModified)(resp)
                  rollupName.foreach(r => Header("X-SODA2-Rollup", r.underlying)(resp))
                  results.run { r =>
                    CJSONWriter.writeCJson(datasetInfo, qrySchema,
                      r, reqRowCount, r.rowCount, dataVersion, lastModified, obfuscateId)(resp)
                  }
                case InfoSuccess(copyNumber, dataVersion, explainInfo) =>
                  implicit val encode = AutomaticJsonEncodeBuilder[ExplainInfo]
                  Header("X-SODA2-CopyNumber", copyNumber.toString)(resp)
                  Header("X-SODA2-DataVersion", dataVersion.toString)(resp)
                  Header("X-SODA2-Secondary-Last-Modified", DateTime.now().toHttpDate)(resp)
                  resp.setContentType("application/json")
                  resp.setCharacterEncoding(utf8EncodingName)

                  val writer = new OutputStreamWriter(resp.getOutputStream, StandardCharsets.UTF_8)
                  JsonUtil.writeJson(writer, Array(explainInfo))
                  writer.flush()
                  writer.close()
              }
          }
      }
    }
  }

  trait QueryResultBall
  case class RowsQueryResult(
    querySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
    dataVersion: Long,
    rows: Managed[CloseableIterator[Row[SoQLValue]] with RowCount]
  ) extends QueryResultBall
  case class InfoQueryResult(
    dataVersion: Long,
    infoMessage: ExplainInfo
  ) extends QueryResultBall

  def execQuery( // scalastyle:ignore method.length parameter.number cyclomatic.complexity
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    datasetInternalName: String,
    datasetInfo: DatasetInfo,
    analysis: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]],
    rowCount: Boolean,
    reqCopy: Option[String],
    rollupName: Option[RollupName],
    obfuscateId: Boolean,
    precondition: Precondition,
    ifModifiedSince: Option[DateTime],
    etagInfo: Option[String],
    queryTimeout: Option[Duration],
    debug: Boolean,
    explain: Boolean,
    analyze: Boolean
  ): QueryResult = {

    def runQuery(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                 latestCopy: CopyInfo,
                 analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]],
                 rowCount: Boolean,
                 queryTimeout: Option[Duration],
                 explain: Boolean,
                 analyze: Boolean): QueryResultBall = {
      val cryptProvider = new CryptProvider(latestCopy.datasetInfo.obfuscationKey)

      val sqlCtx = Map[SqlizerContext, Any](
        SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
                                 else { new ClearNumberRep(cryptProvider) }),
        SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
        SqlizerContext.CaseSensitivity -> caseSensitivity,
        SqlizerContext.LeadingSearch -> leadingSearch
      )
      val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

      for(readCtx <- pgu.datasetReader.openDataset(latestCopy)) {
        val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
        val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
        val qrySchema = querySchema(pgu, analyses.last, latestCopy)
        val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor)
        val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
        val sqlReps = querier.getSqlReps(systemToUserColumnMap)

        // TODO: rethink how reps should be constructed and passed into each chained soql.
        val typeReps = analyses.seq.flatMap { analysis =>
          val qrySchema = querySchema(pgu, analysis, latestCopy)
          qrySchema.map { case (columnId, columnInfo) =>
            columnInfo.typ -> pgu.commonSupport.repFor(columnInfo)
          }
        }.toMap
        val joinCopiesMap = getJoinCopies(pgu, analysis, reqCopy)
        val joinCopies = joinCopiesMap.values.toSeq

        val sqlRepsWithJoin = joinCopiesMap.foldLeft(sqlReps) { (acc, joinCopy) =>
          val (tableName, copyInfo) = joinCopy
          acc ++ getJoinReps(pgu, copyInfo, tableName)
        }

        if (explain) {
          val explain = querier.queryExplain(
            analyses,
            (as: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]], tableName: String) => {
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) + (TableName.PrimaryTable -> tableName)
              Sqlizer.sql((as, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            queryTimeout,
            debug,
            analyze)

          InfoQueryResult(latestCopy.dataVersion, explain)
        } else {
          val results = querier.query(
            analyses,
            (as: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]], tableName: String) => {
              println("TABLENAME: " + tableName)
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) + (TableName.PrimaryTable -> tableName)
              val asFrom = NonEmptySeq(as.head.copy(from = Some(TableName(tableName))), as.tail)
              Sqlizer.sql((asFrom, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            (as: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]], tableName: String) => {
              val tableNameMap = getDatasetTablenameMap(joinCopiesMap) + (TableName.PrimaryTable -> tableName)
              SoQLAnalysisSqlizer.rowCountSql((as, tableNameMap, sqlReps.values.toSeq))(
                sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
            },
            rowCount,
            qryReps,
            queryTimeout,
            debug)

          RowsQueryResult(qrySchema, latestCopy.dataVersion, results)
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
    val lastModified = copy.lastModified

    // Conditional GET handling
    precondition.check(Some(etag), sideEffectFree = true) match {
      case Passed =>
        ifModifiedSince match {
          case Some(ims) if !lastModified.minusMillis(lastModified.getMillisOfSecond).isAfter(ims)
                            && precondition == NoPrecondition =>
            NotModified(Seq(etag))
          case Some(_) | None =>
            try {
              runQuery(pgu, copy, analysis, rowCount, queryTimeout, explain, analyze) match {
                case RowsQueryResult(qrySchema, version, results) =>
                  Success (qrySchema, copy.copyNumber, version, results, etag, lastModified)
                case InfoQueryResult(dataVersion, explainInfo) =>
                  InfoSuccess(copy.copyNumber, dataVersion, explainInfo)
              }
            } catch {
              // ick, but user-canceled requests also fall under this code and those are fine
              case ex: PSQLException if "57014".equals(ex.getSQLState) &&
                  "ERROR: canceling statement due to statement timeout".equals(ex.getMessage) =>
                RequestTimedOut(queryTimeout)
            }
        }
      case FailedBecauseMatch(etags) =>
        NotModified(etags)
      case FailedBecauseNoMatch =>
        PreconditionFailed
    }
  }

  def execQueryUnion( // scalastyle:ignore method.length parameter.number cyclomatic.complexity
                      pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                      datasetInfo: DatasetInfo,
                      analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]]): Unit = {

    val obfuscateId = true
    val latestCopy: CopyInfo = getCopy(pgu, datasetInfo, None)
    val rollupName = None
    val reqCopy = None

    val cryptProvider = new CryptProvider(latestCopy.datasetInfo.obfuscationKey)

    val sqlCtx = Map[SqlizerContext, Any](
      SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
      else { new ClearNumberRep(cryptProvider) }),
      SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
      SqlizerContext.CaseSensitivity -> caseSensitivity,
      SqlizerContext.LeadingSearch -> leadingSearch
    )
    val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

    for(readCtx <- pgu.datasetReader.openDataset(latestCopy)) {
      val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
      val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val qrySchema = querySchema(pgu, analyses.last, latestCopy)
      val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor)
      val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
      val sqlReps = querier.getSqlReps(systemToUserColumnMap)

      // TODO: rethink how reps should be constructed and passed into each chained soql.
      val typeReps = analyses.seq.flatMap { analysis =>
        val qrySchema = querySchema(pgu, analysis, latestCopy)
        qrySchema.map { case (columnId, columnInfo) =>
          columnInfo.typ -> pgu.commonSupport.repFor(columnInfo)
        }
      }.toMap
      println("WIP getJoinCopies")
      val joinCopiesMap = getJoinCopies(pgu, analyses, reqCopy)
      val joinCopies = joinCopiesMap.values.toSeq

      val sqlRepsWithJoin = joinCopiesMap.foldLeft(sqlReps) { (acc, joinCopy) =>
        val (tableName, copyInfo) = joinCopy
        acc ++ getJoinReps(pgu, copyInfo, tableName)
      }

      val tableNameMap = getDatasetTablenameMap(joinCopiesMap) +
        (TableName.PrimaryTable -> latestCopy.dataTableName)  // (TableName("_") -> "_cat") (TableName("_dog") -> "_dog")
      val ParametricSql(sqls, setParams) = Sqlizer.sql((analyses, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
      val sql: String = sqls.head
      println(sqls.mkString("HELLO5: ", ",", ":END"))
      println("HELLO5: " + sql)
     // SELECT _cat.u_name_4,_d1.u_breed_5 FROM _cat JOIN _cat as _d1 ON (_cat.u_name_4 = _d1.u_name_4)
      sqls
    }
  }

  def execQueryBinary( // scalastyle:ignore method.length parameter.number cyclomatic.complexity
                       pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                       datasetInfo: DatasetInfo,
                       banalysis: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]): String = {

    val obfuscateId = true
    val latestCopy: CopyInfo = getCopy(pgu, datasetInfo, None)
    val rollupName = None
    val reqCopy = None
    val analysisSeq = banalysis.seq
    val analyses = NonEmptySeq(analysisSeq.head, analysisSeq.tail)

    val cryptProvider = new CryptProvider(latestCopy.datasetInfo.obfuscationKey)

    val sqlCtx = Map[SqlizerContext, Any](
      SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
      else { new ClearNumberRep(cryptProvider) }),
      SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
      SqlizerContext.CaseSensitivity -> caseSensitivity,
      SqlizerContext.LeadingSearch -> leadingSearch
    )
    val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

    for(readCtx <- pgu.datasetReader.openDataset(latestCopy)) {
      val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
      val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val qrySchema = querySchema(pgu, banalysis.last, latestCopy)
      val qryReps = qrySchema.mapValues(pgu.commonSupport.repFor)
      val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
      val sqlReps = querier.getSqlReps(systemToUserColumnMap)

      // TODO: rethink how reps should be constructed and passed into each chained soql.
      val typeReps = banalysis.seq.flatMap { analysis =>
        val qrySchema = querySchema(pgu, analysis, latestCopy)
        qrySchema.map { case (columnId, columnInfo) =>
          columnInfo.typ -> pgu.commonSupport.repFor(columnInfo)
        }
      }.toMap
      println("WIP getJoinCopies")
      val joinCopiesMap = getJoinCopies(pgu, analyses, reqCopy)
      val joinCopies = joinCopiesMap.values.toSeq

      val sqlRepsWithJoin = joinCopiesMap.foldLeft(sqlReps) { (acc, joinCopy) =>
        val (tableName, copyInfo) = joinCopy
        acc ++ getJoinReps(pgu, copyInfo, tableName)
      }

      val tableNameMap = getDatasetTablenameMap(joinCopiesMap) +
        (TableName.PrimaryTable -> latestCopy.dataTableName)  // (TableName("_") -> "_cat") (TableName("_dog") -> "_dog")

      val ParametricSql(sqls, setParams) = BinarySoQLAnalysisSqlizer.sql((banalysis, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
     // val ParametricSql(sqls, setParams) = SoQLAnalysisSqlizer.sqlbinary(banalysis, tableNameMap, sqlReps.values.toSeq)(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
      val sql: String = sqls.head
      println("HELLO5: " + sql)
      sql
    }
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
        RollupManager.rollupTableName(rollupInfo, copyCtx.copyInfo.dataVersion)
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

  /**
   * @param pgu
   * @param analysis parsed soql
   * @param latest
   * @return a schema for the selected columns
   */
  // TODO: Handle expressions and column aliases.
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

  /**
   * Get lastest schema
   * @param ds Data coordinator dataset id
   * @return Some schema or none
   */
  def getSchema(ds: String, reqCopy: Option[String]): Option[Schema] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }

  def getSchema(id: ResourceName, reqCopy: Option[String]): Option[Schema] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(ResourceName(id.name))
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
      }
    }
  }

  def getSchemaWithFieldName(id: ResourceName, reqCopy: Option[String]): Option[SchemaWithFieldName] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(ResourceName(id.name))
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchemaWithFieldName(readCtx.copyCtx))
      }
    }
  }

  def getRollups(ds: String, reqCopy: Option[String], includeUnmaterialized: Boolean): Option[Iterable[RollupInfo]] = {
    withPgu(dsInfo, truthStoreDatasetInfo = None) { pgu =>
      for {
        datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds, checkDisabled = true)
        datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
      } yield {
        val copy = getCopy(pgu, datasetInfo, reqCopy)
        if (includeUnmaterialized || RollupManager.shouldMaterializeRollups(copy.lifecycleStage)) {
          pgu.datasetMapReader.rollups(copy)
        } else {
          None
        }
      }
    }
  }

  private def getSchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copy: CopyInfo): Schema = {
    pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchema(readCtx.copyCtx))
  }

  private def getSchemaWithFieldName(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copy: CopyInfo): SchemaWithFieldName = {
    pgu.datasetReader.openDataset(copy).run(readCtx => pgu.schemaFinder.getSchemaWithFieldName(readCtx.copyCtx))
  }

  private def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], ds: String, reqCopy: Option[String])
                      : Option[CopyInfo] = {
    for {
      datasetId <- pgu.secondaryDatasetMapReader.datasetIdForInternalName(ds, checkDisabled = true)
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

  private def copyInfoHeaderForSchema(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    copyInfoHeader("Last-Modified")(copyNumber, dataVersion, lastModified)
  }

  private def copyInfoHeaderForRows(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    // TODO: Keeping "Last-Modified" just to make roll out easy.  It will be removed in the next cycle.
    copyInfoHeader("Last-Modified")(copyNumber, dataVersion, lastModified) ~>
      copyInfoHeader("X-SODA2-Secondary-Last-Modified")(copyNumber, dataVersion, lastModified)
  }

  private def copyInfoHeader(lastModifiedHeader: String)(copyNumber: Long, dataVersion: Long, lastModified: DateTime) = {
    Header(lastModifiedHeader, lastModified.toHttpDate) ~>
      Header("X-SODA2-CopyNumber", copyNumber.toString) ~>
      Header("X-SODA2-DataVersion", dataVersion.toString)
  }

  private def getJoinCopies(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                            analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]],
                            reqCopy: Option[String]): Map[TableName, CopyInfo] = {
    val joins = typed.Join.expandJoins(analyses.seq)
    val froms = analyses.seq.flatMap(x => x.from.toSeq)
    val joinTables = joins.map(x => x.from.fromTable) ++ froms
    val joinTableMap = joinTables.flatMap { resourceName =>
      getCopy(pgu, new ResourceName(resourceName.name), reqCopy).map(copy => (resourceName, copy))
    }.toMap

    val allMap = analyses.seq.foldLeft(joinTableMap) { (acc, ana) =>
      ana.more.headOption match {
        case Some(h) =>
          val more = getJoinCopies(pgu, NonEmptySeq(h, ana.more.tail), reqCopy)
          acc ++ more
        case None =>
          acc
      }
    }

    val allMapNoAliases = allMap.map {
      case (tableName, copy) =>
        (tableName.copy(alias = None), copy)
    }

    allMapNoAliases
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
}

object QueryServer extends DynamicPortMap {
  private val logger = Logger[QueryServer]

  sealed abstract class QueryResult
  case class NotModified(etags: Seq[EntityTag]) extends QueryResult
  case object PreconditionFailed extends QueryResult
  case class RequestTimedOut(timeout: Option[Duration]) extends QueryResult
  case class Success(
    qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
    copyNumber: Long,
    dataVersion: Long,
    results: Managed[CloseableIterator[Row[SoQLValue]] with RowCount],
    etag: EntityTag,
    lastModified: DateTime
  ) extends QueryResult
  case class InfoSuccess(
    copyNumber: Long,
    dataVersion: Long,
    explainBlob: ExplainInfo
  ) extends QueryResult

  case class ExplainInfo(query: String, explainPlan: String)

  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if (ifaces.isEmpty) {
      config
    } else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig = ConfigFactory.parseString("com.socrata.soql-server-pg.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryServerConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.soql-server-pg")
  } catch {
    case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def main(args:Array[String]): Unit = {
    val address = config.discovery.address
    val datasourceConfig = new DataSourceConfig(config.getRawConfig("store"), "database")

    implicit object executorResource extends com.rojoma.simplearm.v2.Resource[ExecutorService] {
      def close(a: ExecutorService): Unit = a.shutdown()
    }

    for {
      curator <- CuratorFromConfig(config.curator)
      discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
      pong <- managed(new LivenessCheckResponder(config.livenessCheck))
      executor <- managed(Executors.newCachedThreadPool())
      dsInfo <- DataSourceFromConfig(datasourceConfig)
      conn <- managed(dsInfo.dataSource.getConnection)
      reporter <- MetricsReporter.managed(config.metrics)
    } {
      pong.start()
      val queryServer = new QueryServer(dsInfo, CaseSensitive, config.leadingSearch)
      val advertisedLivenessCheckInfo = new LivenessCheckInfo(hostPort(pong.livenessCheckInfo.getPort),
                                                              pong.livenessCheckInfo.getResponse)
      val auxData = new AuxiliaryData(livenessCheckInfo = Some(advertisedLivenessCheckInfo))
      val curatorBroker = new CuratorBroker(discovery,
                                            address,
                                            config.discovery.name + "." + config.instance,
                                            Some(auxData)) {
        override def register(port: Int): Cookie = {
          super.register(hostPort(port))
        }
      }
      val logOptions = LoggingOptions(LoggerFactory.getLogger(""),
                                      logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))
      val handler = ThreadRenamingHandler(NewLoggingHandler(logOptions)(queryServer.route))
      val server = new SocrataServerJetty(handler,
                     SocrataServerJetty.defaultOptions.
                       withPort(config.port).
                       withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                       withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
                       withBroker(curatorBroker))
      logger.info("starting pg query server")
      server.run()
    }
    logger.info("pg query server exited")
  }
}
