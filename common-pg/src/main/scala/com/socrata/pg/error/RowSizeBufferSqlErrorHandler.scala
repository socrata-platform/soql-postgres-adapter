package com.socrata.pg.error

import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.typesafe.scalalogging.slf4j.Logging
import java.sql.Connection
import org.postgresql.util.PSQLException

object RowSizeBufferSqlErrorContinue extends SqlErrorHandler {
  def guard(conn: Connection)(f: => Unit) {
    RowSizeBufferSqlErrorHandler.guard(conn, None)(f)
  }
}

object RowSizeBufferSqlErrorResync extends SqlErrorHandler {
  def guard(conn: Connection)(f: => Unit) {
    RowSizeBufferSqlErrorHandler.guard(conn, Some(classOf[ResyncSecondaryException]))(f)
  }
}

object RowSizeBufferSqlErrorHandler extends Logging {
  /**
   * Catch index row size buffer exception and optionally rethrow another exception or just ignore it.
   */
  def guard[E <: Exception](conn: Connection, exceptionClass: Option[Class[E]])(f : => Unit) {

    val savepoint = Option(conn.setSavepoint())
    try {
      f
    } catch {
      case ex: PSQLException =>
        isIndexRowSizeError(ex) match {
          case Some(index) =>
            val msg = s"index row size buffer exceeded $index " + exceptionClass.map("rethrow " + _.getName).getOrElse("ignore")
            logger.warn(msg)
            savepoint.foreach(conn.rollback(_))
            exceptionClass.foreach(exClass => throw exClass.getDeclaredConstructor(classOf[String]).newInstance(msg))
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

  def isIndexRowSizeError(ex: PSQLException): Option[String] = {
    ex.getServerErrorMessage.getMessage match {
      case IndexRowSizeError(_, _, index) =>
        Some(index)
      case _ =>
        None
    }
  }

  private val IndexRowSizeError = """index row size (\d+) exceeds maximum (\d+) for index (.*)""".r
}
