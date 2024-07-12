package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}
import scala.concurrent.duration.FiniteDuration

import java.io.{BufferedWriter, OutputStreamWriter, OutputStream, ByteArrayOutputStream, InputStreamReader, IOException}
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement, SQLException, ResultSet}
import java.time.{Clock, LocalDateTime}
import java.util.Base64
import java.util.zip.GZIPOutputStream
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JArray, JString, JValue, JNull, JAtom, JObject}
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
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.{SimpleDocStream, SimpleDocTree, tree => doctree}
import com.socrata.simplecsv.scala.CSVParserBuilder

import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.rollup.RollupInfo
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, Provenance, ScopedResourceName}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLID, SoQLVersion, SoQLInterval, SoQLNull, CJsonWriteRep, NonObfuscatedType}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.sql.Debug
import com.socrata.soql.sqlizer.{Sqlizer, ResultExtractor, SqlizeAnnotation, SqlizerUniverse, SqlNamespaces}
import com.socrata.soql.stdlib.analyzer2.SoQLRewritePassHelpers
import com.socrata.soql.stdlib.analyzer2.rollup.SoQLRollupExact
import com.socrata.datacoordinator.id.{RollupName, DatasetResourceName}
import com.socrata.metrics.rollup.RollupMetrics
import com.socrata.metrics.rollup.events.RollupHit

import com.socrata.pg.analyzer2.{CryptProviderProvider, TransformManager, CostEstimator, CryptProvidersByDatabaseNamesProvenance, RewriteSubcolumns, Namespaces, SoQLExtraContext, SoQLSqlizeAnnotation}
import com.socrata.pg.analyzer2.metatypes.{CopyCache, InputMetaTypes, DatabaseMetaTypes, DatabaseNamesMetaTypes, AugmentedTableName, SoQLMetaTypesExt, Stage}
import com.socrata.pg.analyzer2.ordering._
import com.socrata.pg.store.{PGSecondaryUniverse, SqlUtils, RollupAnalyzer, RollupId}
import com.socrata.pg.query.QueryResult
import com.socrata.pg.server.CJSONWriter
import com.socrata.datacoordinator.common.DbType
import com.socrata.datacoordinator.common.Postgres
import com.socrata.pg.analyzer2.SoQLSqlizer

final abstract class ProcessQuery
object ProcessQuery {
  val log = LoggerFactory.getLogger(classOf[ProcessQuery])

  val errorCodecs = PostgresSoQLError.errorCodecs[PostgresSoQLError]().build

  @AutomaticJsonEncode
  case class SerializationColumnInfo(hint: Option[JValue], isSynthetic: Boolean)

  def apply(
    request: Deserializer.Request,
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    precondition: Precondition,
    rs: ResourceScope
  ): HttpResponse = {
    val analysis = request.analysis
    val passes = request.passes
    val systemContext = request.context

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

    val outOfDate = copyCache.asMap.iterator.flatMap { case (dtn@DatabaseTableName((_, stage)), (copyInfo, _)) =>
      request.auxTableData.get(dtn) match {
        case Some(auxTableData) =>
          val isOutOfDate = copyInfo.dataVersion < auxTableData.truthDataVersion
          if(isOutOfDate) {
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

    val relevantRollups =
      if(request.allowRollups) {
        copyCache.allCopies.flatMap { copy =>
          pgu.datasetMapReader.rollups(copy).
            iterator.
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

    // Before doing rollups, extract our final hints and synthetic
    // info (rollups can/will destroy this).
    val columnsByName = metadataAnalysis.statement.schema.valuesIterator.map { schemaEnt =>
      schemaEnt.name -> SerializationColumnInfo(schemaEnt.hint, schemaEnt.isSynthetic)
    }.toMap
    log.debug("Result column hints: {}", Lazy(JsonUtil.renderJson(columnsByName, pretty = true)))

    // Intermixed rewrites and rollups: rollups are attemted before
    // each group of rewrite passes and after all rewrite passes have
    // happened.
    val nameAnalyses = locally {
      import DatabaseNamesMetaTypes.DebugHelper._

      TransformManager[DatabaseNamesMetaTypes](
        DatabaseNamesMetaTypes.rewriteFrom(dmtState, metadataAnalysis),
        relevantRollups,
        passes,
        new SoQLRewritePassHelpers,
        new SoQLRollupExact(Stringifier.pretty),
        Stringifier.pretty
      )
    }

    case class Sqlized(
      analysis: SoQLAnalysis[DatabaseNamesMetaTypes],
      rollups: Seq[QueryServerRollupInfo],
      sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
      extractor: ResultExtractor[DatabaseNamesMetaTypes],
      systemContextUsed: SoQLExtraContext.SystemContextUsed,
      now: Option[DateTime],
      estimatedCost: Double
    )

    val onlyOne = nameAnalyses.lengthCompare(1) == 0
    val sqlizerResults = nameAnalyses.map { nameAnalysis =>
      SoQLSqlizer(
        nameAnalysis._1,
        new SoQLExtraContext(
          systemContext,
          cryptProviders,
          RewriteSubcolumns[InputMetaTypes](
            request.auxTableData.mapValues(_.locationSubcolumns),
            copyCache
          ),
          SqlUtils.escapeString(pgu.conn, _)
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
    }

    val sqlized = sqlizerResults.iterator.map { sqlizerResult =>
      val (nameAnalysis, Sqlizer.Result(sql, extractor, SoQLExtraContext.Result(systemContextUsed, now, _obfuscatorRequired))) = sqlizerResult
      log.debug("Generated sql:\n{}", sql) // Doc's toString defaults to pretty-printing
      Sqlized(
        nameAnalysis._1,
        relevantRollups.filter { rr => nameAnalysis._2.contains(rr.id) },
        sql,
        extractor,
        systemContextUsed,
        now,
        if(onlyOne) 0.0 else CostEstimator(pgu.conn, sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded)).toString)
      )
    }.minBy(_.estimatedCost)

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
      sqlized.now
    )

    // "last modified" is problematic because it doesn't include
    // modification times of saved views.  At least, at higher levels
    // that actually know about saved views as things that exist and
    // can change over time.
    val lastModified = locally {
      val tablesLastModified = copyCache.mostRecentlyModifiedAt.getOrElse(new DateTime(0L))
      val nowLastModified = sqlized.now.getOrElse(new DateTime(0L))
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

        fulfillRequest(
          columnsByName,
          copyCache,
          dataVersionsBySFName,
          etag,
          outOfDate,
          pgu,
          sqlized.sql,
          sqlized.analysis,
          sqlized.rollups,
          cryptProviders,
          sqlized.extractor,
          lastModified,
          request.debug,
          request.queryTimeout,
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

  def outOfDateHeader(outOfDate: Set[(SFResourceName, Stage)]): HttpServletResponse => Unit =
    Header("X-SODA2-Data-Out-Of-Date", JsonUtil.renderJson(outOfDate.iterator.toSeq.sorted, pretty=false))

  trait QueryServerRollupInfo {
    val rollupDatasetName: DatasetResourceName
    val rollupName: RollupName
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

  // note this _doesn't_ reset the timeout if it throws
  def withTimeout[T](conn: Connection, timeout: Option[FiniteDuration], cond: Boolean = true)(f: => T): T = {
    timeout match {
      case Some(finite) if cond =>
        setTimeout(conn, finite)
        val result = f
        resetTimeout(conn)
        result
      case _ =>
        f
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
    timeout: Option[FiniteDuration]
  ): Either[HttpResponse, String] = {
    handlingSqlErrors(timeout) {
      withTimeout(conn, timeout, cond = analyze) {
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
          sb.toString
        }
      }
    }
  }

  def explainJson(
    conn: Connection,
    query: String,
    analyze: Boolean,
    timeout: Option[FiniteDuration]
  ): Either[HttpResponse, JValue] = {
    handlingSqlErrors(timeout) {
      withTimeout(conn, timeout, cond = analyze) {
        for {
          stmt <- managed(conn.createStatement())
          rs <- managed(stmt.executeQuery(s"EXPLAIN (analyse $analyze, buffers $analyze, format json) $query"))
        } {
          if(!rs.next()) {
            throw new Exception("Should've gotten a row for the explanation")
          }
          redactObfuscateInExplain(JsonReader.fromString(rs.getString(1)))
        }
      }
    }
  }

  def handlingSqlErrors[T](timeout: Option[FiniteDuration])(f: => T): Either[HttpResponse, T] = {
    try {
      Right(f)
    } catch {
      case e: SQLException =>
        QueryResult.QueryRuntimeError(e, timeout) match {
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
    columnsByName: Map[ColumnName, SerializationColumnInfo],
    copyCache: CopyCache[InputMetaTypes],
    dataVersionsBySFName: Seq[((SFResourceName, Stage), Long)],
    etag: EntityTag,
    outOfDate: Set[(SFResourceName, Stage)],
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    sql: Doc[SqlizeAnnotation[DatabaseNamesMetaTypes]],
    nameAnalysis: SoQLAnalysis[DatabaseNamesMetaTypes],
    rollups: Seq[QueryServerRollupInfo],
    cryptProviders: CryptProviderProvider,
    extractor: ResultExtractor[DatabaseNamesMetaTypes],
    lastModified: DateTime,
    debug: Option[Debug],
    timeout: Option[FiniteDuration],
    rs: ResourceScope
  ): HttpResponse = {
    val locale = "en_US"

    for(result <- ResultCache(etag)) {
      log.info("Serving result from cache")
      return buildResponse(result.etag, result.lastModified, result.contentType, outOfDate, os => os.write(result.body))
    }

    val laidOutSql: SimpleDocStream[SqlizeAnnotation[DatabaseNamesMetaTypes]] = sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    val renderedSql = singleLineSql(sql, forDebug = false)
    val debugFields =
      debug.map { debug =>
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
          }.map("explain" -> _)
        ).flatten.toMap
      }

    val preventRun = debug.fold(false)(_.inhibitRun)

    val rows =
      if(preventRun) {
        Iterator.empty
      } else {
        timeout.foreach(setTimeout(pgu.conn, _))
        handlingSqlErrors(timeout) {
          runQuery(pgu.conn, renderedSql, cryptProviders, extractor, rs)
        } match {
          case Left(resp) => return resp
          case Right(rows) => rows
        }
      }

    if(!preventRun) {
      val now = LocalDateTime.now(Clock.systemUTC())
      for(ri <- rollups) {
        RollupMetrics.digest(
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
        cos <- managed(new ResultCache.CachingOutputStream(rawOutputStream))(ResultCache.cosResource(etag, lastModified, contentType))
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
          writer.write("\n,")
          CompactJsonWriter.toWriter(writer, JArray(row))
        }

        writer.write("\n]\n")
      }
    }

    buildResponse(etag, lastModified, contentType, outOfDate, resultStream)
  }

  def buildResponse(etag: EntityTag, lastModified: DateTime, contentType: String, outOfDate: Set[(SFResourceName, Stage)], body: OutputStream => Unit): HttpResponse = {
    ETag(etag) ~>
      LastModified(lastModified) ~>
      ContentType(contentType) ~>
      outOfDateHeader(outOfDate) ~>
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
