package com.socrata.pg.server.analyzer2

import scala.collection.{mutable => scm}

import java.sql.{Connection, PreparedStatement}

import com.rojoma.json.v3.ast.{JArray, JString, JValue}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{AutomaticJsonEncode, JsonUtil}
import com.rojoma.simplearm.v2._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.prettyprint.prelude._
import com.socrata.prettyprint.SimpleDocStream

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLID, SoQLVersion}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.datacoordinator.truth.json.JsonColumnWriteRep
import com.socrata.datacoordinator.common.soql.SoQLRep

import com.socrata.pg.analyzer2.{CryptProviderProvider, Sqlizer}
import com.socrata.pg.store.{PGSecondaryUniverse, SqlUtils}
import com.socrata.pg.server.CJSONWriter

final abstract class ProcessQuery
object ProcessQuery {
  val log = LoggerFactory.getLogger(classOf[ProcessQuery])

  def apply(
    analysis: SoQLAnalysis[InputMetaTypes],
    systemContext: Map[String, String],
    passes: Seq[Seq[rewrite.Pass]],
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    precondition: Precondition,
    rs: ResourceScope
  ): HttpResponse = {
    locally {
      import InputMetaTypes.DebugHelper._
      implicit def cpp = CryptProviderProvider.empty
      log.debug("Received analysis statement:\n{}", Lazy(analysis.statement.debugStr))
    }

    // ok, we have our analysis which is in terms of internal names
    // and column ids, and we want to turn that into having them in
    // terms of copies and columns
    val copyCache = new CopyCache(pgu)
    val metadataAnalysis = DatabaseMetaTypes.rewriteFrom(analysis, copyCache)

    // Then we'll rewrite that analysis in terms of actual physical
    // database names, after building our crypt providers.
    implicit val cryptProviders = CryptProvidersByCanonicalName(metadataAnalysis.statement)
    val nameAnalysis = passes.foldLeft(DatabaseNamesMetaTypes.rewriteFrom(metadataAnalysis)) { (nameAnalysis, batch) =>
      locally {
        import DatabaseNamesMetaTypes.DebugHelper._
        log.debug("Statement before applying rewrites {}:\n{}", batch:Any, Lazy(nameAnalysis.statement.debugStr))
      }
      // here (before doing the applyPasses) and after building
      // nameAnalysis is _probably_ where we want to do rollups
      nameAnalysis.applyPasses(
        batch,
        RewritePasses.isLiteralTrue,
        RewritePasses.isOrderable,
        RewritePasses.and
      )
    }
    locally {
      import DatabaseNamesMetaTypes.DebugHelper._
      log.debug("Statement after applying all rewrites:\n{}", Lazy(nameAnalysis.statement.debugStr))
    }

    val sqlizer = new ActualSqlizer(SqlUtils.escapeString(pgu.conn, _), cryptProviders, systemContext)
    val Sqlizer.Result(sql, extractor, nonliteralSystemContextLookupFound) = sqlizer(nameAnalysis.statement, OrderedMap.empty)
    log.debug("Generated sql:\n{}", sql) // Doc's toString defaults to pretty-printing
    val laidOutSql = sql.group.layoutPretty(LayoutOptions(PageWidth.Unbounded))
    val renderedSql = laidOutSql.toString

    // Our etag will be the hash of:
    //  * this SQL
    //  * the versions
    //  * the provided system context (since it's not guaranteed that all values will occur in the SQL)
    val etag = ETagify(renderedSql, copyCache.orderedVersions, systemContext)
    val lastModified = copyCache.mostRecentlyModifiedAt.getOrElse(new DateTime(0L))

    precondition.check(Some(etag), sideEffectFree = true) match {
      case Precondition.Passed =>
        fulfillRequest(
          if(nonliteralSystemContextLookupFound) Some(systemContext) else None,
          copyCache,
          etag,
          pgu,
          laidOutSql,
          renderedSql,
          nameAnalysis,
          cryptProviders,
          extractor,
          lastModified,
          rs
        )
      case Precondition.FailedBecauseMatch(_) =>
        log.debug("if-none-match resulted in a cache hit")
        NotModified ~> ETag(etag) ~> LastModified(lastModified)
      case Precondition.FailedBecauseNoMatch =>
        log.debug("if-match resulted in a miss")
        PreconditionFailed ~> ETag(etag)
    }
  }

  def fulfillRequest(
    systemContext: Option[Map[String, String]],
    copyCache: CopyCache,
    etag: EntityTag,
    pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
    laidOutSql: SimpleDocStream[com.socrata.pg.analyzer2.SqlizeAnnotation[DatabaseNamesMetaTypes]],
    renderedSql: String,
    nameAnalysis: com.socrata.soql.analyzer2.SoQLAnalysis[DatabaseNamesMetaTypes],
    cryptProviders: com.socrata.pg.analyzer2.CryptProviderProvider,
    extractor: com.socrata.pg.analyzer2.ResultExtractor[DatabaseNamesMetaTypes],
    lastModified: DateTime,
    rs: ResourceScope
  ): HttpResponse = {
    val locale = "en_US"

    systemContext.foreach(setupSystemContext(pgu.conn, _))
    val stmt = rs.open(pgu.conn.createStatement())

    val resultSet =
      try { rs.open(stmt.executeQuery(renderedSql)) }
      catch {
        case e: org.postgresql.util.PSQLException =>
          log.error("Exception running query", e)
          // Sadness: PG _doesn't_ report position info for runtime errors
          // so this position info isn't actually useful :(
          val posInfo = com.socrata.pg.analyzer2.Sqlizer.positionInfo(laidOutSql)
          return InternalServerError
      }

    ETag(etag) ~> LastModified(lastModified) ~> Write("application/json; content-type=utf-8") { writer =>
      writer.write("[{")
      writer.write("\"last_modified\":%s\n ,".format(JString(CJSONWriter.dateTimeFormat.print(lastModified))))
      writer.write("\"locale\":%s\n ,".format(JString(locale)))
      writer.write("\"schema\":")

      @AutomaticJsonEncode
      case class Field(c: ColumnName, t: SoQLType)

      val reformattedSchema = nameAnalysis.statement.schema.values.map { case Statement.SchemaEntry(cn, typ, _isSynthetic) =>
        Field(cn, typ)
      }.toArray

      JsonUtil.writeJson(writer, reformattedSchema)
      writer.write("\n }")

      val width = reformattedSchema.length
      var idReps = new scm.HashMap[Option[String], JsonColumnWriteRep[SoQLType, SoQLValue]]
      var versionReps = new scm.HashMap[Option[String], JsonColumnWriteRep[SoQLType, SoQLValue]]

      def rep(typ: SoQLType, value: SoQLValue): JsonColumnWriteRep[SoQLType, SoQLValue] = {
        value match {
          case id: SoQLID =>
            idReps.get(id.provenance) match {
              case Some(rep) => rep
              case None =>
                val rep = new com.socrata.datacoordinator.common.soql.jsonreps.IDRep(new SoQLID.StringRep(id.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros)))
                idReps += id.provenance -> rep
                rep
            }
          case ver: SoQLVersion =>
            versionReps.get(ver.provenance) match {
              case Some(rep) => rep
              case None =>
                val rep = new com.socrata.datacoordinator.common.soql.jsonreps.VersionRep(new SoQLVersion.StringRep(ver.provenance.flatMap(cryptProviders.forProvenance).getOrElse(CryptProvider.zeros)))
                versionReps += ver.provenance -> rep
                rep
            }
          case other => SoQLRep.jsonRepsMinusIdAndVersion(typ)
        }
      }

      while(resultSet.next()) {
        val result = new Array[JValue](width)
        val row = extractor.extractRow(resultSet)

        var i = 0
        while(i != width) {
          result(i) = rep(reformattedSchema(i).t, row(i)).toJValue(row(i))
          i += 1
        }

        writer.write("\n,")
        CompactJsonWriter.toWriter(writer, JArray(result))
      }

      writer.write("\n]\n")
    }
  }

  def setupSystemContext(conn: Connection, systemContext: Map[String, String]): Unit = {
    using(new ResourceScope) { rs =>
      val BatchSize = 5
      var fullStmt = Option.empty[PreparedStatement]

      def sql(n: Int) =
        Iterator.fill(n) { s"set_config('socrata_system.a' || md5(?), ?, true)" }.
          mkString("SELECT ", ", ", "")

      for(group <- systemContext.iterator.grouped(BatchSize)) {
        val stmt =
          group.length match {
            case BatchSize =>
              fullStmt match {
                case None =>
                  val stmt = rs.open(conn.prepareStatement(sql(BatchSize)))
                  fullStmt = Some(stmt)
                  stmt
                case Some(stmt) =>
                  stmt
              }
            case n =>
              rs.open(conn.prepareStatement(sql(n)))
          }

        for(((varName, varValue), i) <- group.iterator.zipWithIndex) {
          stmt.setString(1 + 2*i, varName)
          stmt.setString(2 + 2*i, varValue)
        }
        stmt.executeQuery().close()
      }
    }
  }
}
