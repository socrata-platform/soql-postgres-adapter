package com.socrata.pg.error

import java.lang.reflect.InvocationTargetException
import java.sql.Connection

import scala.util.{Failure, Success, Try}

import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.typesafe.scalalogging.slf4j.Logging
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

    def resyncIfIndexRowSizeError(ex: PSQLException) {
      isIndexRowSizeError(ex) match {
        case Success(indexOpt) =>
          val index = indexOpt.getOrElse("*unknown*")
          val msg = s"index row size buffer exceeded $index " + exceptionClass.map("rethrow " + _.getName).getOrElse("ignore")
          logger.warn(msg)
          savepoint.foreach(conn.rollback(_))
          exceptionClass.foreach(exClass => throw exClass.getDeclaredConstructor(classOf[String]).newInstance(msg))
        case Failure(ex) =>
          logger.info("got some PSQL exception, trying to roll back...", ex)
          savepoint.foreach(conn.rollback(_))
          throw ex
      }
    }

    try {
      f
    } catch {
      case ex: PSQLException =>
        resyncIfIndexRowSizeError(ex)
      case itex: InvocationTargetException => // copy in calls have sql exceptions wrapped in invocation target exception.
        itex.getCause match {
          case ex: PSQLException => resyncIfIndexRowSizeError(ex)
          case ex => throw ex
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


  /**
   * @param ex Possible PSQLException that is row size related.
   * @return If the exception is row size related, return Success and the index name if known.
   *         Otherwise, return the Failure with the original exception.
   */
  def isIndexRowSizeError(ex: PSQLException): Try[Option[String]] = {
    ex.getSQLState match {
      case "54000" =>
        ex.getServerErrorMessage.getMessage match {
          case IndexRowSizeError(_, _, index) => Success(Some(index))
          case IndexRowSizeErrorWoIndexName(_, _) => Success(None)
          case msg => Failure(ex)
        }
      case unknownState => Failure(ex)
    }
  }

  private val IndexRowSizeError = """index row size (\d+) exceeds maximum (\d+) for index (.*)""".r

  private val IndexRowSizeErrorWoIndexName = """index row requires (\d+) bytes, maximum size is (\d+)""".r
}
