package com.socrata.pg.server.analyzer2

import scala.util.hashing.MurmurHash3

import java.io.OutputStream

import com.rojoma.simplearm.v2._
import org.apache.commons.collections4.map.{AbstractLinkedMap, LRUMap}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.http.server.util.{EntityTag, WeakEntityTag, StrongEntityTag}

object InMemoryResultCache {
  private val log = LoggerFactory.getLogger(classOf[InMemoryResultCache])

  private class WrappedETag(private val underlying: EntityTag) {
    override val hashCode =
      MurmurHash3.bytesHash(underlying.asBytesUnsafe)

    override def equals(o: Any) =
      o match {
        case that: WrappedETag =>
          (this.underlying, that.underlying) match {
            case (a: WeakEntityTag, b: WeakEntityTag) => a.weakCompare(b)
            case (a: StrongEntityTag, b: StrongEntityTag) => a.strongCompare(b)
            case _ => false
          }
        case _ =>
          false
      }
  }
}

final class InMemoryResultCache(maxCache: Int, maxResponseSize: Int) extends ResultCache {
  import ResultCache.Result
  import InMemoryResultCache._

  private val cache = new LRUMap[WrappedETag, Result](maxCache)

  override def apply(etag: EntityTag): Option[Result] = {
    val k = new WrappedETag(etag)
    Option(cache.synchronized { cache.get(k) })
  }

  override def cachingOutputStream(underlying: OutputStream, etag: EntityTag, lastModified: DateTime, contentType: String): Managed[OutputStream] =
    managed(
      new CachingOutputStream(underlying, maxResponseSize) {
        override def save(): Unit = {
          for((bytes, endPtr) <- savedBytes) {
            log.info("Caching result")
            val r = Result(etag, lastModified, contentType, _.write(bytes, 0, endPtr))
            cache.synchronized { cache.put(new WrappedETag(etag), r) }
          }
        }
      }
    )
}
