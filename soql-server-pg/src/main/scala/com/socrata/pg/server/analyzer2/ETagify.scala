package com.socrata.pg.server.analyzer2

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.util.OrJNull.implicits._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}
import com.socrata.http.server.util.{EntityTag, WeakEntityTag}
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.sql.Debug

import com.socrata.pg.analyzer2.metatypes.InputMetaTypes
import com.socrata.pg.analyzer2.ordering._

final abstract class ETagify

object ETagify extends StatementUniverse[InputMetaTypes] {
  private val log = LoggerFactory.getLogger(classOf[ETagify])

  def apply(
    outputColumns: Seq[ColumnName],
    columnsByName: Map[ColumnName, ProcessQuery.SerializationColumnInfo],
    query: String,
    dataVersions: Seq[Long],
    systemContext: Map[String, String],
    fakeCompoundMap: Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]],
    passes: Seq[Seq[rewrite.Pass]],
    allowRollups: Boolean,
    debug: Option[Debug],
    timestampUsage: Option[String]
  ): EntityTag = {
    val hasher = Hasher.newSha256()

    log.debug("Mixing in output column names: {}", Lazy(JsonUtil.renderJson(outputColumns, pretty = false)))
    hasher.hash(outputColumns)

    log.debug("Mixing in output column hints: {}", Lazy(JsonUtil.renderJson(columnsByName, pretty = true)))
    hasher.hash(
      columnsByName.map { case (k, ProcessQuery.SerializationColumnInfo(hint, isSynthetic)) =>
        k.name -> (hint, isSynthetic)
      }.toMap
    )

    log.debug("Mixing in query: {}", JString(query))
    hasher.hash(query)

    log.debug("Mixing in copy versions: {}", dataVersions)
    hasher.hash(dataVersions)

    log.debug("Mixing in system context: {}", Lazy(JsonUtil.renderJson(systemContext, pretty = true)))
    hasher.hash(systemContext)

    log.debug("Mixing in fake compound columns: {}", Lazy(JsonUtil.renderJson(fakeCompoundMap.mapValues(_.mapValues(_.map(_.orJNull)).toSeq).toSeq, pretty = true)))
    hasher.hash(fakeCompoundMap)

    log.debug("Mixing in rewrite passes: {}", Lazy(JsonUtil.renderJson(passes, pretty = true)))
    hasher.hash(passes)

    log.debug("Mixing in allow-rollups: {}", allowRollups)
    hasher.hash(allowRollups)

    log.debug("Mixing in debug: {}", debug)
    hasher.hash(debug)

    log.debug("Mixing in timestamp usage: {}", timestampUsage)
    hasher.hash(timestampUsage)

    // Should this be strong or weak?  I'm choosing weak here because
    // I don't think we actually guarantee two calls will be
    // byte-for-byte identical...
    WeakEntityTag(hasher.digest())
  }
}
