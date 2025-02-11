package com.socrata.pg.analyzer2

import scala.collection.{mutable => scm}

import java.sql.Connection

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.simplearm.v2._
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime, LocalDate}
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory

import com.socrata.soql.analyzer2.{MetaTypes, LiteralValue, AtomicPositionInfo}
import com.socrata.soql.sqlizer.{Rep, MetaTypesExt}
import com.socrata.soql.functions.SoQLTypeInfo
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLFixedTimestamp, SoQLFloatingTimestamp}

trait TimestampProvider {
  def now: DateTime

  // These all return None if zoneName does not name a valid time zone
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

  abstract class SimpleTimestampProvider extends TimestampProvider {
    private val tracker = new scm.TreeMap[String, (Any, String)]

    protected def cached[T](tag: String, f: T => String)(value: => Option[T]): Option[T] = {
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

    private val nowValue = DateTime.now().withZone(DateTimeZone.UTC).withMillisOfSecond(0)
    private var nowUsed = false
    protected def actualNow = {
      nowUsed = true
      nowValue
    }

    protected def nowInZoneIntl(name: String): Option[LocalDateTime]

    override final def now = {
      nowUsed = true
      cached[DateTime]("now", _.getMillis.toString)(Some(actualNow)).get
    }

    private def iso(ldt: LocalDateTime): String = ISODateTimeFormat.dateTime.print(ldt)
    private def isoDate(ld: LocalDate): String = ISODateTimeFormat.date.print(ld)

    override final def nowInZone(zoneName: String) =
      cached("now/" + zoneName, iso) { nowInZoneIntl(zoneName) }

    override final def nowTruncDay(zoneName: String) = {
      cached("trunc day/" + zoneName, iso) { nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0)) }
    }

    override final def nowTruncMonth(zoneName: String) =
      cached("trunc month/" + zoneName, iso) {
        nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0).withDayOfMonth(1))
      }

    override final def nowTruncYear(zoneName: String) =
      cached("trunc year/" + zoneName, iso) {
        nowInZoneIntl(zoneName).map(_.withTime(0, 0, 0, 0).withDayOfMonth(1).withMonthOfYear(1))
      }

    override final def nowDate(zoneName: String) =
      cached("date/" + zoneName, isoDate) {
        nowInZoneIntl(zoneName).map(_.toLocalDate)
      }

    override final def nowExtractDay(zoneName: String) =
      cached[Int]("day", _.toString) {
        nowInZoneIntl(zoneName).map(_.getDayOfMonth)
      }

    override final def nowExtractMonth(zoneName: String) =
      cached[Int]("month", _.toString) {
        nowInZoneIntl(zoneName).map(_.getMonthOfYear)
      }

    override final def nowExtractYear(zoneName: String) =
      cached[Int]("year", _.toString) {
        nowInZoneIntl(zoneName).map(_.getYear)
      }

    override final def finish() = {
      new TimestampUsage {
        override val etagInfo: Option[String] =
          Some(CompactJsonWriter.toString(JObject(tracker.mapValues { v => JString(v._2) })))

        override val effectiveLastModified: Option[DateTime] =
          if(nowUsed) None
          else Some(nowValue)
      }
    }
  }

  // This is used in tests.  We don't have a database connection to
  // run against, so instead just implement it in terms of joda-time's
  // timezone database.
  class InProcess extends SimpleTimestampProvider {
    private def zone(name: String): Option[DateTimeZone] =
      try {
        Some(DateTimeZone.forID(name))
      } catch {
        case e: IllegalArgumentException =>
          None
      }

    protected override def nowInZoneIntl(name: String) = {
      zone(name).map { z =>
        actualNow.withZone(z).toLocalDateTime
      }
    }
  }

  object Postgresql {
    private val log = LoggerFactory.getLogger(classOf[Postgresql])
  }

  class Postgresql(conn: Connection) extends SimpleTimestampProvider {
    import SoQLTypeInfo.hasType
    import Postgresql.log

    private lazy val knownNames =
      for {
        stmt <- managed(conn.prepareStatement("SELECT name from pg_catalog.pg_timezone_names"))
        rs <- managed(stmt.executeQuery())
      } {
        val rb = Set.newBuilder[String]
        while(rs.next()) {
          rb += rs.getString(1)
        }
        rb.result()
      }

    private val zoned = new scm.HashMap[String, LocalDateTime]

    private val floatingRep = new com.socrata.datacoordinator.common.soql.sqlreps.FloatingTimestampRep("")

    protected override def nowInZoneIntl(name: String): Option[LocalDateTime] = {
      zoned.get(name).orElse {
        if(knownNames(name)) {
          log.debug("Converting {} into zone {}", actualNow:Any, JString(name))
          val result =
            for {
              stmt <- managed(conn.prepareStatement("select (? :: timestamp with time zone) at time zone ?"))
                .and(_.setString(1, SoQLFixedTimestamp.StringRep(actualNow)))
                .and(_.setString(2, name))
              rs <- managed(stmt.executeQuery())
            } {
              if(!rs.next()) throw new Exception("SELECT for zone conversion did not return a row?")
              floatingRep.fromResultSet(rs, 1).asInstanceOf[SoQLFloatingTimestamp].value
            }
          log.debug("Converted {} to {} in {}", actualNow, result, JString(name))
          zoned += name -> result
          Some(result)
        } else {
          log.debug("Postgresql does not know about zone {}; declining the conversion", JString(name))
          None
        }
      }
    }
  }
}
