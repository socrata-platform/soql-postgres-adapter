package com.socrata.pg.server.analyzer2

import scala.util.hashing.MurmurHash3

import org.apache.commons.collections4.map.LRUMap
import org.joda.time.DateTime

import com.socrata.http.server.util.{EntityTag, WeakEntityTag, StrongEntityTag}
import com.socrata.soql.analyzer2._

object ResultCache {
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

  def isCacheable(analysis: SoQLAnalysis[_]): Boolean = {
    // a response is cacheable if it's known to be small - currently,
    // "known to be small" means "it's an aggregate query with no
    // GROUP BY clause (i.e., the entire query is one group)".
    analysis.statement match {
      case sel: Select[_] if sel.isAggregated && sel.groupBy.isEmpty => true
      case _ => false
    }
  }
}
