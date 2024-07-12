package com.socrata.pg.server.analyzer2

import java.io.OutputStream

import com.rojoma.simplearm.v2._
import org.joda.time.DateTime

import com.socrata.http.server.util.EntityTag

trait ResultCache {
  def apply(etag: EntityTag): Option[ResultCache.Result]
  def cachingOutputStream(underlying: OutputStream, etag: EntityTag, lastModified: DateTime, contentType: String): Managed[OutputStream]
}

object ResultCache {
  case class Result(etag: EntityTag, lastModified: DateTime, contentType: String, body: Array[Byte])
}
