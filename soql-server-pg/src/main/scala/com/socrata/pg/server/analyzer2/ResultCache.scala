package com.socrata.pg.server.analyzer2

import java.io.{InputStream, OutputStream}

import com.rojoma.simplearm.v2._
import org.joda.time.DateTime

import com.socrata.http.server.util.EntityTag

trait ResultCache {
  def apply(key: EntityTag): Option[ResultCache.Result]
  def cachingOutputStream(underlying: OutputStream, etag: EntityTag, lastModified: DateTime, contentType: String, startTime: MonotonicTime): Managed[OutputStream]

  // An outputstream that forwards to `underlying`, but will save up
  // to `limit` written bytes, which can be retrieved by calling
  // `takeResult`.
  protected abstract class CachingOutputStream(underlying: OutputStream, limit: Int) extends OutputStream {
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

    def abandon(): Unit = {
      bytes = null
    }

    // Returns `None` if it has been explicitly abandoned or if too
    // much was written.
    protected def savedBytes: Option[(Array[Byte], Int)] =
      Option(bytes).map((_, endPtr))

    def save(): Unit
  }

  protected implicit val cosResource =
    new Resource[CachingOutputStream] {
      override def closeAbnormally(cos: CachingOutputStream, e: Throwable): Unit = {
        cos.abandon()
        cos.close()
      }

      override def close(cos: CachingOutputStream): Unit = {
        cos.close()
        cos.save()
      }
    }
}

object ResultCache {
  case class Result(etag: EntityTag, lastModified: DateTime, contentType: String, originalDurationMS: Long, body: OutputStream => Unit)
}
