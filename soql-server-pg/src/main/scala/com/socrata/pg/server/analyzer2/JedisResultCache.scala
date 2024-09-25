package com.socrata.pg.server.analyzer2

import java.io.{OutputStream, ByteArrayOutputStream, DataOutputStream, IOException, ByteArrayInputStream, DataInputStream}
import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.ast.JString
import com.rojoma.simplearm.v2._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool}

import com.socrata.http.server.util.{EntityTag, EntityTagParser, EntityTagRenderer}

object JedisResultCache {
  private val log = LoggerFactory.getLogger(classOf[JedisResultCache])

  private class WritableByteArrayInputStream(bs: Array[Byte], off: Int, len: Int) extends ByteArrayInputStream(bs, off, len) {
    def this(bs: Array[Byte]) = this(bs, 0, bs.length)

    def writeRemaining(to: OutputStream): Unit = {
      to.write(buf, pos, count-pos)
    }
  }
}

final class JedisResultCache(jedisPool: JedisPool, maxValueSize: Int) extends ResultCache {
  import JedisResultCache._

  private implicit object JedisResource extends Resource[Jedis] {
    def close(j: Jedis) = jedisPool.returnResource(j)
  }

  private def withJedis[T](f: Jedis => T): T = using(jedisPool.getResource())(f)

  private def key(etag: EntityTag) = EntityTagRenderer(etag).getBytes(StandardCharsets.UTF_8)

  private val VERSION = 1

  private def parse(etag: EntityTag, data: Array[Byte]): Option[ResultCache.Result] = {
    val bais = new WritableByteArrayInputStream(data)
    val in = new DataInputStream(bais)
    try {
      if(in.readInt() != VERSION) return None
      val lastModified = new DateTime(in.readLong())
      val contentType = in.readUTF()
      val len = in.readInt()
      if(len != bais.available) throw new Exception("Size mismatch")
      Some(ResultCache.Result(etag, lastModified, contentType, bais.writeRemaining))
    } catch {
      case e: Exception =>
        log.warn("Malformed data; ignoring the cached value", e)
        None
    }
  }

  private def render(lastModified: DateTime, contentType: String, body: Array[Byte], bodyEnd: Int): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val out = new DataOutputStream(baos)

    out.writeInt(VERSION)
    out.writeLong(lastModified.getMillis)
    out.writeUTF(contentType)
    out.writeInt(bodyEnd)
    out.write(body, 0, bodyEnd)
    out.close()

    baos.toByteArray
  }

  override def apply(etag: EntityTag): Option[ResultCache.Result] = {
    withJedis { jedis =>
      try {
        Option(jedis.get(key(etag))).flatMap(parse(etag, _))
      } catch {
        case e: Exception =>
          log.warn("Failed to fetch key {} from redis; treating as a cache miss", etag:Any, e)
          None
      }
    }
  }

  override def cachingOutputStream(underlying: OutputStream, etag: EntityTag, lastModified: DateTime, contentType: String): Managed[OutputStream] =
    managed(
      new CachingOutputStream(underlying, maxValueSize) {
        override def save(): Unit = {
          for((bytes, endPtr) <- savedBytes) {
            withJedis { jedis =>
              try {
                jedis.set(key(etag), render(lastModified, contentType, bytes, endPtr))
              } catch {
                case e: Exception =>
                  log.warn("Failed to set key {} in redis; not caching", etag:Any, e)
              }
            }
          }
        }
      }
    )
}
