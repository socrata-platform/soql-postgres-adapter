package com.socrata.pg.server.analyzer2

import java.io.OutputStream

import com.rojoma.simplearm.v2._
import org.joda.time.DateTime

import com.socrata.http.server.util.EntityTag

object NoopResultCache extends ResultCache {
  override def apply(etag: EntityTag) = None

  override def cachingOutputStream(underlying: OutputStream, etag: EntityTag, lastModified: DateTime, contentType: String, startTime: MonotonicTime): Managed[OutputStream] =
    new Managed[OutputStream] {
      override def run[T](f: OutputStream => T): T = f(underlying)
    }
}
