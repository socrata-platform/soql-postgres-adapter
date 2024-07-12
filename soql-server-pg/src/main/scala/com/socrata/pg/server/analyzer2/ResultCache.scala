package com.socrata.pg.server.analyzer2

import scala.util.hashing.MurmurHash3

import java.io.OutputStream
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.rojoma.simplearm.v2.Resource
import org.apache.commons.collections4.map.{AbstractLinkedMap, LRUMap}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.http.server.util.{EntityTag, WeakEntityTag, StrongEntityTag}

final abstract class ResultCache

object ResultCache {
  val log = LoggerFactory.getLogger(classOf[ResultCache])

  private val uselessCache = new AtomicLong(0)

  case class Result(etag: EntityTag, lastModified: DateTime, contentType: String, body: Array[Byte])

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

  private val cache = new LRUMap[WrappedETag, Result](10000)

  def apply(etag: EntityTag): Option[Result] = {
    val k = new WrappedETag(etag)
    Option(cache.synchronized { cache.get(k) })
  }

  def save(etag: EntityTag, lastModified: DateTime, contentType: String, body: Array[Byte]): Unit = {
    val k = new WrappedETag(etag)
    val r = Result(etag, lastModified, contentType, body)
    cache.synchronized { cache.put(k, r) }
  }

  class CachingOutputStream(underlying: OutputStream) extends OutputStream {
    private val limit = 10240
    private var bytes = new Array[Byte](limit)
    private var endPtr = 0

    override def write(b: Int): Unit = {
      underlying.write(b)
      if(bytes ne null) {
        if(endPtr != limit) {
          bytes(endPtr) = b.toByte
          endPtr += 1
        } else { // response was too big, stop caching
          bytes = null
        }
      }
    }

    override def write(bs: Array[Byte]): Unit = {
      write(bs, 0, bs.length)
    }

    override def write(bs: Array[Byte], off: Int, len: Int): Unit = {
      underlying.write(bs, off, len)
      if(bytes ne null) {
        if(len <= limit - endPtr) {
          System.arraycopy(bs, off, bytes, endPtr, len)
          endPtr += len
        } else { // resposne was too big, stop caching
          bytes = null
        }
      }
    }

    override def close(): Unit = {
      underlying.close()
    }

    override def flush(): Unit = {
      underlying.flush()
    }

    def save(etag: EntityTag, lastModified: DateTime, contentType: String): Unit = {
      if(bytes ne null) {
        log.info("Caching result")
        val result = new Array[Byte](endPtr)
        System.arraycopy(bytes, 0, result, 0, endPtr)
        bytes = null
        ResultCache.save(etag, lastModified, contentType, result)
      }
    }

    def abandon(): Unit = {
      bytes = null
    }
  }

  def cosResource(etag: EntityTag, lastModified: DateTime, contentType: String): Resource[CachingOutputStream] =
    new Resource[CachingOutputStream] {
      override def closeAbnormally(cos: CachingOutputStream, e: Throwable): Unit = {
        cos.abandon()
        cos.close()
      }

      override def close(cos: CachingOutputStream): Unit = {
        cos.close()
        cos.save(etag, lastModified, contentType)
      }
    }
}
