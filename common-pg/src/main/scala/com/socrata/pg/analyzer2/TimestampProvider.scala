package com.socrata.pg.analyzer2

import scala.collection.{mutable => scm}

import java.sql.Connection

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat

trait TimestampProvider {
  def now: DateTime

  def nowInZone(zoneName: String): Option[LocalDateTime]
  def nowTruncDay(zoneName: String): Option[LocalDateTime]
  def nowTruncMonth(zoneName: String): Option[LocalDateTime]
  def nowTruncYear(zoneName: String): Option[LocalDateTime]
  def nowDate(zoneName: String): Option[LocalDate]
  def nowExtractDay(zoneName: String): Option[Int]
  def nowExtractMonth(zoneName: String): Option[Int]
  def nowExtractYear(zoneName: String): Option[Int]

  def finish(): TimestampUsage
}

trait TimestampUsage {
  val etagInfo: Option[String]
  val effectiveLastModified: Option[DateTime]
}
object TimestampUsage {
  object NoUsage extends TimestampUsage {
    override val etagInfo = None
    override val effectiveLastModified = None
  }
}

object TimestampProvider {
  // This is used by the rollup manager; get_utc_time is forbidden in
  // rollups and any use of it should have been caught beforehand.
  object Forbid extends TimestampProvider {
    private def explode(): Nothing =
      throw new Exception("get_utc_time() should not have been used in this context")
    override def now = explode()
    override def nowInZone(zoneName: String) = explode()
    override def nowTruncDay(zoneName: String) = explode()
    override def nowTruncMonth(zoneName: String) = explode()
    override def nowTruncYear(zoneName: String) = explode()
    override def nowDate(zoneName: String) = explode()
    override def nowExtractDay(zoneName: String) = explode()
    override def nowExtractMonth(zoneName: String) = explode()
    override def nowExtractYear(zoneName: String) = explode()
    override def finish() = TimestampUsage.NoUsage
  }

  // This is used in tests.  We don't have a database connection to
  // run against, so instead just implement it in terms of joda-time's
  // timezone database.
  class InProcess extends TimestampProvider {
    private val tracker = new scm.TreeMap[String, (Any, String)]

    private def cached[T](tag: String, f: T => String)(value: => Option[T]): Option[T] = {
      tracker.get(tag) match {
        case Some((x, _)) =>
          Some(x.asInstanceOf[T])
        case None =>
          val result: Option[T] = value
          result.foreach { r =>
            tracker += tag -> (result, f(r))
          }
          result
      }
    }

    private val actualNow = DateTime.now().withZone(DateTimeZone.UTC).withMillisOfSecond(0)
    private var nowUsed = false

    private def zone(name: String): Option[DateTimeZone] =
      try {
        Some(DateTimeZone.forID(name))
      } catch {
        case e: IllegalArgumentException =>
          None
      }

    private def nowInZoneIntl(name: String) = {
      nowUsed = true
      zone(name).map { z => actualNow.withZone(z).toLocalDateTime }
    }

    override def now = {
      nowUsed = true
      cached[DateTime]("now", _.getMillis.toString)(Some(actualNow)).get
    }

    private def iso(ldt: LocalDateTime): String = ISODateTimeFormat.dateTime.print(ldt)
    private def isoDate(ld: LocalDate): String = ISODateTimeFormat.date.print(ld)

    override def nowInZone(zoneName: String) =
      cached("now/" + zoneName, iso) { nowInZoneIntl(zoneName) }

    override def nowTruncDay(zoneName: String) = {
      cached("trunc day/" + zoneName, iso) { nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0)) }
    }

    override def nowTruncMonth(zoneName: String) =
      cached("trunc month/" + zoneName, iso) {
        nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0).withDayOfMonth(1))
      }

    override def nowTruncYear(zoneName: String) =
      cached("trunc year/" + zoneName, iso) {
        nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0).withDayOfMonth(1).withMonthOfYear(1))
      }

    override def nowDate(zoneName: String) =
      cached("date/" + zoneName, isoDate) {
        nowInZoneIntl(zoneName).map(_.toLocalDate)
      }

    override def nowExtractDay(zoneName: String) =
      cached[Int]("day", _.toString) {
        nowInZoneIntl(zoneName).map(_.getDayOfMonth)
      }

    override def nowExtractMonth(zoneName: String) =
      cached[Int]("month", _.toString) {
        nowInZoneIntl(zoneName).map(_.getMonthOfYear)
      }

    override def nowExtractYear(zoneName: String) =
      cached[Int]("year", _.toString) {
        nowInZoneIntl(zoneName).map(_.getYear)
      }

    override def finish() = {
      new TimestampUsage {
        override val etagInfo: Option[String] =
          Some(CompactJsonWriter.toString(JObject(tracker.mapValues { v => JString(v._2) })))

        override val effectiveLastModified: Option[DateTime] =
          if(nowUsed) None
          else Some(actualNow)
      }
    }
  }

  class Postgresql(conn: Connection) extends TimestampProvider {
    private def explode(): Nothing =
      throw new Exception("get_utc_time() should not have been used in this context")

    private val actualNow = DateTime.now().withZone(DateTimeZone.UTC).withMillisOfSecond(0)
    private var nowUsed = false

    def now: DateTime = {
      nowUsed = true
      actualNow
    }
    override def nowInZone(zoneName: String) = explode()
    override def nowTruncDay(zoneName: String) = explode()
    override def nowTruncMonth(zoneName: String) = explode()
    override def nowTruncYear(zoneName: String) = explode()
    override def nowDate(zoneName: String) = explode()
    override def nowExtractDay(zoneName: String) = explode()
    override def nowExtractMonth(zoneName: String) = explode()
    override def nowExtractYear(zoneName: String) = explode()
    override def finish() = explode()
  }
}
