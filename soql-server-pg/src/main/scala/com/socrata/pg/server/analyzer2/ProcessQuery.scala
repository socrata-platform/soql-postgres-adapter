package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}
import scala.concurrent.duration.FiniteDuration
import java.io.{BufferedWriter, ByteArrayOutputStream, IOException, InputStreamReader, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.time.{Clock, LocalDateTime}
import java.util.Base64
import java.util.zip.GZIPOutputStream
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.v3.ast.{JArray, JAtom, JNull, JObject, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.json.v3.util.{AutomaticJsonEncode, JsonUtil}
import com.rojoma.simplearm.v2._
import org.joda.time.DateTime
import org.postgresql.PGConnection
import org.postgresql.copy.PGCopyInputStream
import org.slf4j.LoggerFactory
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{EntityTag, Precondition, WeakEntityTag}
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.{SimpleDocStream, SimpleDocTree, tree => doctree}
import com.socrata.simplecsv.scala.CSVParserBuilder
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, Provenance, ResourceName, ScopedResourceName}
import com.socrata.soql.types.{CJsonWriteRep, NonObfuscatedType, SoQLID, SoQLInterval, SoQLNull, SoQLType, SoQLValue, SoQLVersion}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.sql.Debug
import com.socrata.soql.sqlizer.{ResultExtractor, SqlNamespaces, SqlizeAnnotation, Sqlizer, SqlizerUniverse}
import com.socrata.soql.stdlib.analyzer2.SoQLRewritePassHelpers
import com.socrata.soql.stdlib.analyzer2.rollup.SoQLRollupExact
import com.socrata.datacoordinator.id.{DatasetResourceName, RollupName}
import com.socrata.datacoordinator.truth.metadata.LifecycleStage
import com.socrata.pg.analyzer2.{CostEstimator, CryptProviderProvider, CryptProvidersByDatabaseNamesProvenance, Namespaces, RewriteSubcolumns, SoQLExtraContext, SoQLSqlizeAnnotation, TimestampProvider, TimestampUsage}
import com.socrata.pg.analyzer2.metatypes.{AugmentedTableName, CopyCache, DatabaseMetaTypes, DatabaseNamesMetaTypes, InputMetaTypes, SoQLMetaTypesExt, Stage}
import com.socrata.pg.analyzer2.ordering._
import com.socrata.pg.store.{PGSecondaryUniverse, RollupAnalyzer, RollupId, SqlUtils}
import com.socrata.pg.query.QueryResult
import com.socrata.pg.server.CJSONWriter
import com.socrata.datacoordinator.common.DbType
import com.socrata.datacoordinator.common.Postgres
import com.socrata.metrics.{DatasetRollup, DatasetRollupCost, Metric, RollupHit, RollupSelection}
import com.socrata.pg.analyzer2.SoQLSqlizer
import com.socrata.metrics.Timing

object ProcessQuery {
  val log = LoggerFactory.getLogger(classOf[ProcessQuery])

  val errorCodecs = PostgresSoQLError.errorCodecs[PostgresSoQLError]().build

  @AutomaticJsonEncode
  case class SerializationColumnInfo(hint: Option[JValue], isSynthetic: Boolean)

  trait TimeoutHandle {
    def ping(): Unit // Call this whenever productive work is observed to reset the timeout
    def cancelled: Boolean // true if the manager has issued a pg_cancel_backend on us
    def duration: Option[FiniteDuration]
  }

  object TimeoutHandle {
    class Noop extends TimeoutHandle {
      override def ping() = {}
      override def cancelled = false
      override def duration = None
    }
    object Noop extends Noop
  }

  trait TimeoutManager {
    def register(pid: Int, timeout: FiniteDuration, rs: ResourceScope): TimeoutHandle
  }

  object TimeoutManager {
    object Noop extends TimeoutManager {
      def register(pid: Int, timeout: FiniteDuration, rs: ResourceScope): TimeoutHandle = rs.openUnmanaged(new TimeoutHandle.Noop)
    }
  }
}

class ProcessQuery(resultCache: ResultCache, timeoutManager: ProcessQuery.TimeoutManager) {
  import ProcessQuery._

  def apply(
    request: Deserializer.Request,
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    precondition: Precondition,
    rs: ResourceScope
  ): HttpResponse = {
    val startTime = MonotonicTime.now()

    val analysis = request.analysis
    val passes = request.passes
    val systemContext = request.context

    val timeoutHandle =
      request.queryTimeout match {
        case Some(timeout) =>
          for {
            stmt <- managed(pgu.conn.prepareStatement("SELECT pg_backend_pid()"))
            result <- managed(stmt.executeQuery())
          } {
            if (!result.next()) throw new Exception("SELECT pg_backend_pid() should have returned exactly one row")
            timeoutManager.register(result.getInt(1), timeout, rs)
          }
        case None =>
          TimeoutHandle.Noop
      }

    locally {
      import InputMetaTypes.DebugHelper._
      implicit def cpp = CryptProviderProvider.empty

      log.debug("Received analysis statement:\n{}", Lazy(analysis.statement.debugStr))
    }

    // ok, we have our analysis which is in terms of internal names
    // and column ids, and we want to turn that into having them in
    // terms of copies and columns
    val copyCache = new InputMetaTypes.CopyCache(pgu)
    val dmtState = new DatabaseMetaTypes
    val metadataAnalysis = dmtState.rewriteFrom(analysis, copyCache, InputMetaTypes.provenanceMapper) // this populates the copy cache

    timeoutHandle.ping()

    val outOfDate = copyCache.asMap.iterator.flatMap { case (dtn@DatabaseTableName((_, stage)), (copyInfo, _)) =>
      request.auxTableData.get(dtn) match {
        case Some(auxTableData) =>
          val isOutOfDate = copyInfo.dataVersion < auxTableData.truthDataVersion
          if (isOutOfDate) {
            Some((auxTableData.sfResourceName, stage))
          } else {
            None
          }
        case None =>
          log.warn("No aux data found for table {}?!?!?", dtn)
          None
      }
    }.toSet

    // Then we'll rewrite that analysis in terms of actual physical
    // database names, after building our crypt providers.
    implicit val cryptProviders: CryptProviderProvider = CryptProvidersByDatabaseNamesProvenance(metadataAnalysis.statement)

    // but first. produce a string of the analysis for etagging purposes
    val stringifiedAnalysis = locally {
      import InputMetaTypes.DebugHelper._
      analysis.statement.debugDoc.layoutPretty(LayoutOptions(PageWidth.Unbounded)).toString
    }

    val (relevantRollups, relevantRollupsDuration) =
      Timing.TimedResultReturning {
        if (request.allowRollups) {
          copyCache.allCopies.flatMap { copy =>
            pgu.datasetMapReader.rollups(copy).
              iterator.
              map { rollup => timeoutHandle.ping(); rollup }.
              filter { rollup => rollup.isNewAnalyzer && tableExists(pgu, rollup.tableName) }.
              flatMap { rollup => RollupAnalyzer(pgu, copy, rollup).map((rollup, _)) }.
              map { case (rollup, (_foundTables, analysis, _locationSubcolumnsMap, _cryptProvider)) =>
                // don't care about the other things because:
                //   * we're not going to be sqlizing this rollup
                //   * any referenced datasets we already know about
                new RollupInfo[DatabaseNamesMetaTypes, RollupId] with QueryServerRollupInfo {
                  override val id = rollup.systemId
                  override val statement = analysis.statement
                  override val resourceName = ScopedResourceName(-1, ResourceName(s"rollup:${rollup.copyInfo.datasetInfo.systemId}/${rollup.copyInfo.systemId}/${rollup.name.underlying}"))
                  override val databaseName = DatabaseTableName(AugmentedTableName.RollupTable(rollup.tableName))

                  // Needed for metrics + response
                  override val rollupDatasetName = rollup.copyInfo.datasetInfo.resourceName
                  override val rollupDatasetStage = rollup.copyInfo.lifecycleStage
                  override val rollupName = rollup.name

                  private val columns = statement.schema.keysIterator.toArray

                  override def databaseColumnNameOfIndex(idx: Int) =
                    DatabaseColumnName(Namespaces.rawAutoColumnName(columns(idx)))
                }
              }
          }
        } else {
          Nil
        }
      }((results,dur)=>{
        (results,dur)
      })


    // Before doing rollups, extract our final hints and synthetic
    // info (rollups can/will destroy this).
    val columnsByName = metadataAnalysis.statement.schema.valuesIterator.map { schemaEnt =>
      schemaEnt.name -> SerializationColumnInfo(schemaEnt.hint, schemaEnt.isSynthetic)
    }.toMap
    log.debug("Result column hints: {}", Lazy(JsonUtil.renderJson(columnsByName, pretty = true)))

    // Intermixed rewrites and rollups: rollups are attemted before
    // each group of rewrite passes and after all rewrite passes have
    // happened.
    val (nameAnalyses, nameAnalysesDuration) = locally {
      import DatabaseNamesMetaTypes.DebugHelper._

      val transformManager =
        new TransformManager[DatabaseNamesMetaTypes, RollupId](
          new SoQLRollupExact(Stringifier.pretty),
          new SoQLRewritePassHelpers,
          Stringifier.pretty
        )

      Timing.TimedResultReturning{
        transformManager(
          DatabaseNamesMetaTypes.rewriteFrom(dmtState, metadataAnalysis),
          relevantRollups,
          passes
        )
      }((results,dur)=>(results,dur))
    }

    case class Sqlized(
      analysis: SoQLAnalysis[DatabaseNamesMetaTypes],
      rollups: Seq[QueryServerRollupInfo],
      sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
      extractor: ResultExtractor[DatabaseNamesMetaTypes],
      systemContextUsed: SoQLExtraContext.SystemContextUsed,
      timestampUsage: TimestampUsage,
      estimatedCost: Double
    )

    val onlyOne = nameAnalyses.lengthCompare(1) == 0
    val sqlizerResults = nameAnalyses.map { nameAnalysis =>
      timeoutHandle.ping()
      SoQLSqlizer(
        nameAnalysis._1,
        new SoQLExtraContext(
          systemContext,
          cryptProviders,
          RewriteSubcolumns[InputMetaTypes](
            request.auxTableData.mapValues(_.locationSubcolumns),
            copyCache
          ),
          SqlUtils.escapeString(pgu.conn, _),
          new TimestampProvider.Postgresql(pgu.conn)
        )
      ) match {
        case Right(result) => (nameAnalysis, result)
        case Left(nothing) => nothing
      }
    }

    // This has to happen before cost-estimation, because the EXPLAIN
    // it does will depend on the obfuscator temp function existing.
    if(sqlizerResults.exists(_._2.extraContextResult.obfuscatorRequired)) {
      ObfuscatorHelper.setupObfuscators(pgu.conn, cryptProviders.allProviders.mapValues(_.key))
      timeoutHandle.ping()
    }

    // Ditto for the dynamic context function
    for {
      // Any dynamic access will, of course, contain the whole
      // context, so we only need to find the first one
      sysContext <- sqlizerResults.iterator
        .map(_._2.extraContextResult.systemContextUsed)
        .collect { case SoQLExtraContext.SystemContextUsed.Dynamic(dyn) => dyn }
        .buffered
        .headOption
    } {
      DynamicSystemContextHelper.setupDynamicSystemContext(pgu.conn, sysContext)
      timeoutHandle.ping()
    }

    val (sqlized, rollupStats) =
      Timing.TimedResultReturning {
        sqlizerResults.iterator.map { sqlizerResult =>
          val (nameAnalysis, Sqlizer.Result(sql, extractor, SoQLExtraContext.Result(systemContextUsed, tsu, _obfuscatorRequired))) = sqlizerResult
          log.debug("Generated sql:\n{}", sql) // Doc's toString defaults to pretty-printing
          timeoutHandle.ping()
          Sqlized(
            nameAnalysis._1,
            relevantRollups.filter { rr => nameAnalysis._2.contains(rr.id) },
            sql,
            extractor,
            systemContextUsed,
            tsu,
            if (onlyOne) 0.0 else CostEstimator(pgu.conn, sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded)).toString)
          )
        }.foldLeft((Option.empty[Sqlized], Map.empty[Set[((DatasetResourceName, LifecycleStage), RollupName)], Double])) {
          // We're selecting the cheapest sqlization _and_ keeping track
          // of the estimated costs of all sqlizations.
          case ((None, _), sqlized) =>
            (Some(sqlized), Map(sqlized.rollups.map(_.namePair).toSet -> sqlized.estimatedCost))
          case ((Some(previousMin), acc), sqlized) =>
            val newMin = if (sqlized.estimatedCost < previousMin.estimatedCost) {
              sqlized
            } else {
              previousMin
            }

            (Some(newMin), acc + (sqlized.rollups.map(_.namePair).toSet -> sqlized.estimatedCost))
        }
      }((results,dur)=>{
        val (Some(sqlized), rollupStats) = results
          if(relevantRollups.nonEmpty){
            // If we had candidates but no relevant rollups then we do not need metrics - since most of the metrics will be relating simply to general rewrite passes
            val candidates: Seq[DatasetRollupCost] = rollupStats.map { case (set, cost) =>
              DatasetRollupCost(
                group = set.map { case ((dataset, _), rollup) =>
                  DatasetRollup(dataset.underlying, rollup.underlying)
                },
                cost = cost
              )
            }.toSeq
            val selection: Seq[DatasetRollup] = sqlized.rollups.map(_.namePair).map{ case ((dataset, _),rollup) => DatasetRollup(dataset.underlying,rollup.underlying)}
            Metric.digest(RollupSelection(candidates, selection, dur.plus(nameAnalysesDuration).plus(relevantRollupsDuration), LocalDateTime.now()))
          }
          (sqlized, rollupStats)
        })

    // Our etag will be the hash of the inputs that affect the result of the query
    val etag = ETagify(
      analysis.statement.schema.valuesIterator.map(_.name).toSeq,
      columnsByName,
      stringifiedAnalysis,
      copyCache.orderedVersions,
      sqlized.systemContextUsed.relevantEtagContext,
      request.auxTableData.mapValues(_.locationSubcolumns),
      passes,
      request.allowRollups,
      request.debug,
      sqlized.timestampUsage.etagInfo
    )

    // "last modified" is problematic because it doesn't include
    // modification times of saved views.  At least, at higher levels
    // that actually know about saved views as things that exist and
    // can change over time.
    val lastModified = locally {
      val tablesLastModified = copyCache.mostRecentlyModifiedAt.getOrElse(new DateTime(0L))
      val nowLastModified = sqlized.timestampUsage.effectiveLastModified.getOrElse(new DateTime(0L))
      if(tablesLastModified.getMillis > nowLastModified.getMillis) {
        tablesLastModified
      } else {
        nowLastModified
      }
    }

    precondition.check(Some(etag), sideEffectFree = true) match {
      case Precondition.Passed =>
        val dataVersionsBySFName: Seq[((SFResourceName, Stage), Long)] =
          copyCache.asMap.iterator.flatMap { case (dtn@DatabaseTableName((internalName, stage)), (copyInfo, _)) =>
            request.auxTableData.get(dtn) match {
              case Some(atd) =>
                Some((atd.sfResourceName, stage) -> copyInfo.dataVersion)
              case None =>
                log.warn("No aux data found for table {}?!?!?", dtn)
                None
            }
          }.toVector.sortBy(_._1)

        val debug = request.debug.getOrElse(Debug.default)

        fulfillRequest(
          startTime,
          columnsByName,
          copyCache,
          dataVersionsBySFName,
          etag,
          outOfDate,
          pgu,
          sqlized.sql,
          sqlized.analysis,
          sqlized.rollups,
          rollupStats,
          cryptProviders,
          sqlized.extractor,
          lastModified,
          debug,
          timeoutHandle,
          rs
        )
      case Precondition.FailedBecauseMatch(_) =>
        log.debug("if-none-match resulted in a cache hit")
        NotModified ~> ETag(etag) ~> LastModified(lastModified) ~> outOfDateHeader(outOfDate)
      case Precondition.FailedBecauseNoMatch =>
        log.debug("if-match resulted in a miss")
        PreconditionFailed ~> ETag(etag)
    }
  }

  def cachedHeader(cached: Boolean): HttpServletResponse => Unit =
    Header("X-SODA2-Cached", JsonUtil.renderJson(cached, pretty = false))
  def outOfDateHeader(outOfDate: Set[(SFResourceName, Stage)]): HttpServletResponse => Unit =
    Header("X-SODA2-Data-Out-Of-Date", JsonUtil.renderJson(outOfDate.iterator.toSeq.sorted, pretty=false))

  trait QueryServerRollupInfo {
    val rollupDatasetName: DatasetResourceName
    val rollupDatasetStage: LifecycleStage
    val rollupName: RollupName

    def namePair = ((rollupDatasetName, rollupDatasetStage), rollupName)
  }

  def tableExists(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], table: String): Boolean = {
    for {
      stmt <- managed(pgu.conn.prepareStatement("SELECT exists(select 1 from pg_tables where schemaname = 'public' and tablename = ?)")).
        and(_.setString(1, table))
      rs <- managed(stmt.executeQuery())
    } {
      if(!rs.next()) {
        throw new Exception("`SELECT exists(..)` returned nothing??")
      }
      rs.getBoolean(1)
    }
  }

  def setTimeout(conn: Connection, timeout: FiniteDuration): Unit = {
    val timeoutMs = timeout.toMillis.min(Int.MaxValue).max(1).toInt
    using(conn.createStatement()) { stmt =>
      stmt.execute(s"SET LOCAL statement_timeout TO $timeoutMs")
    }
  }

  def resetTimeout(conn: Connection): Unit = {
    using(conn.createStatement()) { stmt =>
      stmt.execute("SET LOCAL statement_timeout TO DEFAULT")
    }
  }

  def redactObfuscateInExplain(s: String): String = {
    // each obfuscator is 4172 bytes, which are then doubled by
    // hex-encoding them, giving 8344
    s.replaceAll("""obfuscate\('\\x[0-9a-f]{8344}+'""", "obfuscate(/*redacted*/")
  }

  def redactObfuscateInExplain(j: JValue): JValue =
    j match {
      case JString(s) => JString(redactObfuscateInExplain(s))
      case otherAtom: JAtom => otherAtom
      case JArray(elems) => JArray(elems.map(redactObfuscateInExplain(_)))
      case JObject(fields) => JObject(fields.mapValues(redactObfuscateInExplain(_)))
    }

  def explainText(
    conn: Connection,
    query: String,
    analyze: Boolean,
    timeout: TimeoutHandle
  ): Either[HttpResponse, String] = {
    handlingSqlErrors(timeout) {
      for {
        stmt <- managed(conn.createStatement())
        rs <- managed(stmt.executeQuery(s"EXPLAIN (analyze $analyze, buffers $analyze, format text) $query"))
      } {
        val sb = new StringBuilder
        var didOne = false
        while(rs.next()) {
          if(didOne) sb.append('\n')
          else didOne = true
          sb.append(redactObfuscateInExplain(rs.getString(1)))
        }
        timeout.ping()
        sb.toString
      }
    }
  }

  def explainJson(
    conn: Connection,
    query: String,
    analyze: Boolean,
    timeout: TimeoutHandle
  ): Either[HttpResponse, JValue] = {
    handlingSqlErrors(timeout) {
      for {
        stmt <- managed(conn.createStatement())
        rs <- managed(stmt.executeQuery(s"EXPLAIN (analyse $analyze, buffers $analyze, format json) $query"))
      } {
        if(!rs.next()) {
          throw new Exception("Should've gotten a row for the explanation")
        }
        timeout.ping()
        redactObfuscateInExplain(JsonReader.fromString(rs.getString(1)))
      }
    }
  }

  def handlingSqlErrors[T](timeout: TimeoutHandle)(f: => T): Either[HttpResponse, T] = {
    try {
      Right(f)
    } catch {
      case e: SQLException if timeout.cancelled =>
        Left(RequestTimeout ~> Json(errorCodecs.encode(PostgresSoQLError.RequestTimedOut(timeout.duration))))
      case e: SQLException =>
        QueryResult.QueryRuntimeError(e, None) match {
          case Some(QueryResult.RequestTimedOut(timeout)) =>
            Left(RequestTimeout ~> Json(errorCodecs.encode(PostgresSoQLError.RequestTimedOut(timeout))))
          case Some(QueryResult.QueryError(description)) =>
            // Sadness: PG _doesn't_ report position info for runtime
            // errors so this position info isn't actually useful,
            // even if the sql were available here...
            // val posInfo = Sqlizer.positionInfo(laidOutSql)

            Left(BadRequest ~> Json(errorCodecs.encode(PostgresSoQLError.QueryError(description))))
          case None =>
            log.error("Exception running query", e)
            Left(InternalServerError)
        }
    }
  }

  def fulfillRequest(
    startTime: MonotonicTime,
    columnsByName: Map[ColumnName, SerializationColumnInfo],
    copyCache: CopyCache[InputMetaTypes],
    dataVersionsBySFName: Seq[((SFResourceName, Stage), Long)],
    etag: EntityTag,
    outOfDate: Set[(SFResourceName, Stage)],
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
    nameAnalysis: SoQLAnalysis[DatabaseNamesMetaTypes],
    rollups: Seq[QueryServerRollupInfo],
    rollupStats: Map[Set[((DatasetResourceName, LifecycleStage), RollupName)], Double],
    cryptProviders: CryptProviderProvider,
    extractor: ResultExtractor[DatabaseNamesMetaTypes],
    lastModified: DateTime,
    debug: Debug,
    timeout: TimeoutHandle,
    rs: ResourceScope
  ): HttpResponse = {
    val locale = "en_US"

    if(debug.useCache) {
      for(result <- resultCache(etag)) {
        return buildResponse(
          result.etag,
          result.lastModified,
          result.contentType,
          outOfDate,
          { outputStream =>
            try {
              result.body(outputStream)
            } finally {
              log.info("Served result from cache (original took {}ms, cached took {}ms)", result.originalDurationMS, startTime.elapsed().toMilliseconds)
            }
          },
          cached = true
        )
      }
    }

    val laidOutSql: SimpleDocStream[SqlizeAnnotation[DatabaseNamesMetaTypes]] = sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    val renderedSql = singleLineSql(sql, forDebug = false)
    val debugFields =
      Some(
        Seq(
          debug.sql.map { fmt =>
            val s =
              fmt match {
                case Debug.Sql.Format.Compact =>
                  singleLineSql(sql, forDebug = true)
                case Debug.Sql.Format.Pretty =>
                  prettierSql(nameAnalysis, sql, forDebug = true)
              }
            JString(s)
          }.map("sql" -> _),
          debug.explain.map { case Debug.Explain(analyze, format) =>
            val explainResult =
              format match {
                case Debug.Explain.Format.Text =>
                  explainText(pgu.conn, renderedSql, analyze, timeout).map(JString(_))
                case Debug.Explain.Format.Json =>
                  explainJson(pgu.conn, renderedSql, analyze, timeout)
              }
            explainResult match {
              case Left(errorResponse) => return errorResponse
              case Right(json) => json
            }
          }.map("explain" -> _),
          if(debug.rollupStats) {
            val jsonifiedRollupStats = json"""{
              "candidates": ${rollupStats.toVector.sortBy(_._2)},
              "selected": ${rollups.map(_.namePair).toVector}
            }"""
            Some("rollupStats" -> jsonifiedRollupStats)
          } else {
            None
          }
        ).flatten.toMap
      ).filter(_.nonEmpty)

    val rows =
      if(debug.inhibitRun) {
        Iterator.empty
      } else {
        timeout.ping()
        handlingSqlErrors(timeout) {
          runQuery(pgu.conn, renderedSql, cryptProviders, extractor, rs)
        } match {
          case Left(resp) => return resp
          case Right(rows) => rows
        }
      }

    timeout.ping()

    if(!debug.inhibitRun) {
      val now = LocalDateTime.now(Clock.systemUTC())
      for(ri <- rollups) {
        log.info("New-analyzer rollup hit: {}/{}", ri.rollupDatasetName.underlying:Any, ri.rollupName.underlying)
        Metric.digest(
          RollupHit(
            ri.rollupDatasetName.underlying,
            ri.rollupName.underlying,
            now
          )
        )
      }
    }

    val contentType = "application/x-socrata-gzipped-cjson"

    val resultStream: OutputStream => Unit = { rawOutputStream =>
      for {
        cos <- if(debug.useCache) resultCache.cachingOutputStream(rawOutputStream, etag, lastModified, contentType, startTime)
               else unmanaged(rawOutputStream)
        gzos <- managed(new GZIPOutputStream(cos))
        osw <- managed(new OutputStreamWriter(gzos, StandardCharsets.UTF_8))
        writer <- managed(new BufferedWriter(osw))
      } {
        writer.write("[{")
        writer.write("\"dataVersions\":%s\n ,".format(JsonUtil.renderJson(dataVersionsBySFName, pretty = false)))
        for(debugFields <- debugFields) {
          writer.write("\"debug\":%s\n ,".format(JsonUtil.renderJson(debugFields, pretty = false)))
        }
        writer.write("\"fingerprint\":%s\n ,".format(JString(Base64.getUrlEncoder.withoutPadding.encodeToString(etag.asBytes))))
        writer.write("\"last_modified\":%s\n ,".format(JString(CJSONWriter.dateTimeFormat.print(lastModified))))
        writer.write("\"locale\":%s\n ,".format(JString(locale)))
        writer.write("\"schema\":")

        @AutomaticJsonEncode
        case class Field(c: ColumnName, t: SoQLType, h: Option[JValue], s: Boolean)

        val reformattedSchema = nameAnalysis.statement.schema.values.map { case Statement.SchemaEntry(cn, typ, _hint, isSynthetic) =>
          // not using the analysis's schema's hints because it's
          // post-rollup, so any hints inherited from parts of the
          // query served by a rollup will have been destroyed.
          Field(cn, typ, columnsByName.get(cn).flatMap(_.hint), columnsByName.get(cn).map(_.isSynthetic).getOrElse(false))
        }.toArray

        JsonUtil.writeJson(writer, reformattedSchema)
        writer.write("\n }")

        for(row <- rows) {
          timeout.ping()
          writer.write("\n,")
          CompactJsonWriter.toWriter(writer, JArray(row))
        }

        writer.write("\n]\n")
      }
    }

    buildResponse(etag, lastModified, contentType, outOfDate, resultStream, cached = false)
  }

  def buildResponse(etag: EntityTag, lastModified: DateTime, contentType: String, outOfDate: Set[(SFResourceName, Stage)], body: OutputStream => Unit, cached: Boolean): HttpResponse = {
    ETag(etag) ~>
      LastModified(lastModified) ~>
      ContentType(contentType) ~>
      outOfDateHeader(outOfDate) ~>
      cachedHeader(cached) ~>
      Stream(body)
  }

  def runQuery(
    conn: Connection,
    sql: String,
    cryptProviders: CryptProviderProvider,
    extractor: ResultExtractor[DatabaseNamesMetaTypes],
    rs: ResourceScope
  ): Iterator[Array[JValue]] = {
    if(extractor.schema.values.exists(_ == SoQLInterval)) {
      // CSV parser expects intervals to be rendered as ISO 8601,
      // which is not the default, so set it for this txn.
      using(conn.createStatement()) { stmt =>
        stmt.execute("SET LOCAL intervalstyle = 'iso_8601'")
      }
    }

    val it = new Iterator[Array[JValue]] {
      private val stream = rs.open(new PGCopyInputStream(conn.unwrap(classOf[PGConnection]), s"COPY ($sql) TO STDOUT WITH (FORMAT csv, HEADER false, ENCODING 'utf-8')"))
      private val reader = rs.open(new InputStreamReader(stream, StandardCharsets.UTF_8), transitiveClose = List(stream))
      private val csv = rs.open(CSVParserBuilder.Postgresql.build(reader).iterator, transitiveClose = List(reader))

      private val idReps = new scm.HashMap[Option[Provenance], CJsonWriteRep[SoQLID]]
      private val versionReps = new scm.HashMap[Option[Provenance], CJsonWriteRep[SoQLVersion]]

      private val types = extractor.schema.values.toArray
      private val width = types.length

      val columnEncoders: Array[SoQLValue => JValue] = {
        def enc(f: SoQLValue => JValue) = f // type inference helper

        types.map {
          case SoQLID =>
            enc {
              case id: SoQLID =>
                val rep =
                  idReps.get(id.provenance) match {
                    case Some(rep) => rep
                    case None =>
                      val rep = SoQLID.cjsonRep(id.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros), true)
                      idReps += id.provenance -> rep
                      rep
                  }
                rep.toJValue(id)
              case SoQLNull =>
                JNull
              case other =>
                throw new Exception("Expected SoQLID or null, got " + other)
            }

          case SoQLVersion =>
            enc {
              case ver: SoQLVersion =>
                val rep =
                  versionReps.get(ver.provenance) match {
                    case Some(rep) =>
                      rep
                    case None =>
                      val rep = SoQLVersion.cjsonRep(ver.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros))
                      versionReps += ver.provenance -> rep
                      rep
                  }
                rep.toJValue(ver)
              case SoQLNull =>
                JNull
              case other =>
                throw new Exception("Expected SoQLVersion or null, got " + other)
            }

          case typ: NonObfuscatedType =>
            typ.cjsonRep.asErasedCJsonWriteRep.toJValue _
        }
      }

      private def unwrapSQLException[T](f: => T): T = {
        // PGCopyInputStream wraps SQLExceptions in IOExceptions
        try {
          f
        } catch {
          case e: IOException if e.getCause.isInstanceOf[SQLException] =>
            throw e.getCause
        }
      }

      override def hasNext: Boolean = unwrapSQLException(csv.hasNext)

      def next(): Array[JValue] = {
        val result = new Array[JValue](width)
        val row = extractor.extractCsvRow(unwrapSQLException(csv.next()))

        var i = 0
        while(i != width) {
          result(i) = columnEncoders(i)(row(i))
          i += 1
        }

        result
      }
    }

    it.hasNext // force execution to start

    it
  }

  def singleLineSql[MT <: MetaTypes with SoQLMetaTypesExt](doc: Doc[SqlizeAnnotation[MT]], forDebug: Boolean): String = {
    def walk(sb: StringBuilder, tree: SimpleDocTree[SqlizeAnnotation[MT]]): Unit = {
      tree match {
        case doctree.Empty =>
          // nothing
        case doctree.Char(c) =>
          sb.append(c)
        case doctree.Text(s) =>
          sb.append(s)
        case doctree.Line(n) =>
          sb.append('\n')
          var i = 0
          while(i < n) {
            sb.append(' ')
            i += 1
          }
        case doctree.Ann(a, doc) if doc.isInstanceOf[doctree.Ann[_]] =>
          walk(sb, doc)
        case doctree.Ann(a, doc) =>
          a match {
            case SqlizeAnnotation.Expression(_) =>
              walk(sb, doc)
            case SqlizeAnnotation.Table(_) =>
              walk(sb, doc)
            case SqlizeAnnotation.OutputName(_) =>
              walk(sb, doc)
            case SqlizeAnnotation.Custom(SoQLSqlizeAnnotation.Hidden) =>
              if(forDebug) {
                sb.append("/* hidden */")
                walk(new StringBuilder(), doc)
              } else {
                walk(sb, doc)
              }
          }
        case doctree.Concat(elems) =>
          elems.foreach(walk(sb, _))
      }
    }

    val sb = new StringBuilder
    walk(sb, doc.group.layoutPretty(LayoutOptions(PageWidth.Unbounded)).asTree)
    sb.toString
  }

  def prettierSql[MT <: MetaTypes with SoQLMetaTypesExt](analysis: SoQLAnalysis[MT], doc: Doc[SqlizeAnnotation[MT]], forDebug: Boolean): String = {
    val map = new LabelMap(analysis)

    def walk(sb: StringBuilder, tree: SimpleDocTree[SqlizeAnnotation[MT]]): Unit = {
      tree match {
        case doctree.Empty =>
          // nothing
        case doctree.Char(c) =>
          sb.append(c)
        case doctree.Text(s) =>
          sb.append(s)
        case doctree.Line(n) =>
          sb.append('\n')
          var i = 0
          while(i < n) {
            sb.append(' ')
            i += 1
          }
        case doctree.Ann(a, doc) if doc.isInstanceOf[doctree.Ann[_]] =>
          walk(sb, doc)
        case doctree.Ann(a, doc) =>
          a match {
            case SqlizeAnnotation.Expression(VirtualColumn(tLabel, cLabel, _)) =>
              walk(sb, doc)
              for((rn, cn) <- map.virtualColumnNames.get((tLabel, cLabel))) {
                sb.append(" /* ")
                for(rn <- rn) {
                  sb.append(rn.name)
                  sb.append('.')
                }
                sb.append(cn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.Expression(PhysicalColumn(tLabel, _, cLabel, _)) =>
              walk(sb, doc)
              for((rn, cn) <- map.physicalColumnNames.get((tLabel, cLabel))) {
                sb.append(" /* ")
                for(rn <- rn) {
                  sb.append(rn.name)
                  sb.append('.')
                }
                sb.append(cn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.Expression(expr) =>
              walk(sb, doc)
            case SqlizeAnnotation.Table(tLabel) =>
              walk(sb, doc)
              for {
                rn <- map.tableNames.get(tLabel)
                rn <- rn
              } {
                sb.append(" /* ")
                sb.append(rn.name)
                sb.append(" */")
              }
            case SqlizeAnnotation.OutputName(name) =>
              walk(sb, doc)
              sb.append(" /* ")
              sb.append(name.name)
              sb.append(" */")

            case SqlizeAnnotation.Custom(SoQLSqlizeAnnotation.Hidden) =>
              if(forDebug) {
                sb.append("/* hidden */")
                walk(new StringBuilder(), doc)
              } else {
                walk(sb, doc)
              }
          }
        case doctree.Concat(elems) =>
          elems.foreach(walk(sb, _))
      }
    }

    val sb = new StringBuilder
    walk(sb, doc.layoutSmart().asTree)
    sb.toString
  }

  class LabelMap[MT <: MetaTypes](analysis: SoQLAnalysis[MT]) extends StatementUniverse[MT] {
    val tableNames = new scm.HashMap[AutoTableLabel, Option[ResourceName]]
    val virtualColumnNames = new scm.HashMap[(AutoTableLabel, ColumnLabel), (Option[ResourceName], ColumnName)]
    val physicalColumnNames = new scm.HashMap[(AutoTableLabel, DatabaseColumnName), (Option[ResourceName], ColumnName)]

    walkStmt(analysis.statement, None)

    private def walkStmt(stmt: Statement, currentLabel: Option[(AutoTableLabel, Option[ResourceName])]): Unit =
      stmt match {
        case CombinedTables(_, left, right) =>
          walkStmt(left, currentLabel)
          walkStmt(right, None)
        case CTE(defLbl, defAlias, defQ, _matHint, useQ) =>
          walkStmt(defQ, Some((defLbl, defAlias)))
          walkStmt(useQ, currentLabel)
        case v: Values =>
          for {
            (tl, rn) <- currentLabel
            (cl, schemaEntry) <- v.schema
          } {
            virtualColumnNames += (tl, cl) -> (rn, schemaEntry.name)
          }
        case sel: Select =>
          walkFrom(sel.from)
          for {
            (tl, rn) <- currentLabel
            (cl, ne) <- sel.selectList
          } {
            virtualColumnNames += (tl, cl) -> (rn, ne.name)
          }
      }

    private def walkFrom(from: From): Unit =
      from.reduce[Unit](
        walkAtomicFrom,
        { (_, join) => walkAtomicFrom(join.right) }
      )

    private def walkAtomicFrom(from: AtomicFrom): Unit =
      from match {
        case ft: FromTable =>
          tableNames += ft.label -> Some(ft.definiteResourceName.name)
          for((dcn, ne) <- ft.columns) {
            physicalColumnNames += (ft.label, dcn) -> (Some(ft.definiteResourceName.name), ne.name)
          }
        case fsr: FromSingleRow =>
          tableNames += fsr.label -> None
        case fs: FromStatement =>
          tableNames += fs.label -> fs.resourceName.map(_.name)
          walkStmt(fs.statement, Some((fs.label, fs.resourceName.map(_.name))))
      }
  }
}
