package com.socrata.pg.server.analyzer2

import scala.collection.immutable.SortedMap

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer

import com.socrata.http.server.util.{EntityTag, WeakEntityTag}

object ETagify {
  private val chars = "0123456789abcdef".toCharArray
  private val nonUTF8 = 0xff.toByte

  def apply(sql: String, copyVersions: Seq[Long], systemContext: Map[String, String]): EntityTag = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(sql.getBytes(StandardCharsets.UTF_8))
    md.update(nonUTF8)

    val buffer = ByteBuffer.allocate(8)
    buffer.asIntBuffer.put(copyVersions.size)
    md.update(buffer)
    for(copyVersion <- copyVersions) {
      buffer.clear()
      buffer.asLongBuffer.put(copyVersion)
      md.update(buffer)
    }

    val canonicalizedSystemContext = systemContext.toSeq.sorted
    for((k, v) <- systemContext) {
      md.update(k.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)
      md.update(v.getBytes(StandardCharsets.UTF_8))
      md.update(nonUTF8)
    }

    // Should this be strong or weak?  I'm choosing weak here because
    // I don't think we actually guarantee two calls will be
    // byte-for-byte identical...
    WeakEntityTag(md.digest())
  }
}
