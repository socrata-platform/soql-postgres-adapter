package com.socrata.pg.error

import com.typesafe.scalalogging.slf4j.Logging
import java.sql.Connection
import org.postgresql.util.PSQLException
import scala.util.matching.Regex


case class SqlErrorPattern(sqlState: String, message: Regex)

class SqlErrorHelper(patterns: SqlErrorPattern*) extends Logging {

  def guard[E <: Exception](conn: Connection, exceptionClass: Option[Class[E]])(f : => Unit) {

    val savepoint = Option(conn.setSavepoint())
    try {
      f
    } catch {
      case ex: PSQLException =>
        patterns.find { (pat: SqlErrorPattern) =>
          pat.sqlState == ex.getSQLState &&
          pat.message.findFirstMatchIn(ex.getServerErrorMessage.getMessage).isDefined } match {
          case Some(rx) =>
            logger.warn("guard", ex)
            savepoint.foreach(conn.rollback(_))
            exceptionClass.foreach(exClass => throw exClass.getDeclaredConstructor(classOf[String]).newInstance(ex.getMessage))
          case None =>
            logger.info("got some PSQL exception, trying to roll back...", ex)
            savepoint.foreach(conn.rollback(_))
            throw ex
        }
    } finally {
      try {
        savepoint.foreach(conn.releaseSavepoint)
      } catch {
        case ex: PSQLException =>
          logger.error("release savepoint", ex)
      }
    }
  }
}
