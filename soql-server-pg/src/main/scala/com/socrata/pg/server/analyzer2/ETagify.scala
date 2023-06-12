package com.socrata.pg.server.analyzer2

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import com.rojoma.json.v3.ast.JString
import org.slf4j.LoggerFactory

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}
import com.socrata.http.server.util.{EntityTag, WeakEntityTag}
import com.socrata.soql.analyzer2._

import com.socrata.pg.analyzer2.SqlizerUniverse

final abstract class ETagify

object ETagify extends SqlizerUniverse[InputMetaTypes] {
  private val log = LoggerFactory.getLogger(classOf[ETagify])

  private val chars = "0123456789abcdef".toCharArray
  private val nonUTF8 = 0xff.toByte   // Neither of these byte appear in
  private val nonUTF8_2 = 0xfe.toByte // valid UTF-8

  def apply(
    query: String,
    dataVersions: Seq[Long],
    systemContext: Map[String, String],
    fakeCompoundMap: Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]
  ): EntityTag = {
    val md = MessageDigest.getInstance("SHA-256")

    log.debug("Mixing in query: {}", JString(query))
    md.update(query.getBytes(StandardCharsets.UTF_8))
    md.update(nonUTF8)

    val buffer = ByteBuffer.allocate(8 * (dataVersions.size + 1))

    locally {
      log.debug("Mixing in copy versions: {}", dataVersions)
      val longBuffer = buffer.asLongBuffer
      longBuffer.put(dataVersions.size)
      for(dataVersion <- dataVersions) {
        longBuffer.put(dataVersion)
      }
    }
    md.update(buffer)

    val canonicalizedSystemContext = systemContext.toSeq.sorted
    buffer.clear()
    buffer.asLongBuffer.put(canonicalizedSystemContext.size)
    md.update(buffer)
    for((k, v) <- canonicalizedSystemContext) {
      log.debug("Mixing in system context item: {}: {}", JString(k) : Any, JString(v))
      md.update(k.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)
      md.update(v.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)
    }

    val canonicalizedFakeCompoundMap = fakeCompoundMap.toSeq.sortBy {
      case (DatabaseTableName((DatasetInternalName(instance, dsid), Stage(stage))), _cols) =>
        (instance, dsid.underlying, stage)
    }
    buffer.clear()
    buffer.asLongBuffer.put(canonicalizedFakeCompoundMap.size)
    md.update(buffer)
    for((DatabaseTableName((datasetInternalName, Stage(stage))), cols) <- canonicalizedFakeCompoundMap) {
      log.debug("Mixing in fake compound columns for dataset {}/{}", datasetInternalName : Any, JString(stage))

      md.update(datasetInternalName.underlying.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)
      md.update(stage.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)

      val canonicalizedCols = cols.toSeq.sortBy {
        case (DatabaseColumnName(col), _subcols) =>
          col.underlying
      }
      buffer.clear()
      buffer.asLongBuffer.put(canonicalizedCols.size)
      md.update(buffer)
      for((DatabaseColumnName(col), subcols) <- canonicalizedCols) {
        log.debug("Mixing in fake compound columns for column {}: {}", col : Any, subcols)

        md.update(col.underlying.getBytes(StandardCharsets.UTF_8))
        md.update(nonUTF8)

        buffer.clear()
        buffer.asLongBuffer.put(subcols.size)
        md.update(buffer)
        for(subcol <- subcols) {
          subcol match {
            case Some(DatabaseColumnName(ucid)) =>
              md.update(ucid.underlying.getBytes(StandardCharsets.UTF_8))
              md.update(nonUTF8)
            case None =>
              md.update(nonUTF8_2)
          }
        }
      }
    }

    // Should this be strong or weak?  I'm choosing weak here because
    // I don't think we actually guarantee two calls will be
    // byte-for-byte identical...
    WeakEntityTag(md.digest())
  }
}
