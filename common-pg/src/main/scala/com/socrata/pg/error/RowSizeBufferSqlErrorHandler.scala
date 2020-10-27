package com.socrata.pg.error

import java.lang.reflect.InvocationTargetException
import java.sql.{BatchUpdateException, Connection}

import scala.util.{Failure, Success, Try}
import com.socrata.datacoordinator.secondary.ResyncSecondaryException
import com.typesafe.scalalogging.Logger
import org.postgresql.util.PSQLException

object RowSizeBufferSqlErrorContinue extends SqlErrorHandler {
  def guard(conn: Connection)(f: => Unit): Unit = {
    RowSizeBufferSqlErrorHandler.guard(conn, None)(f)
  }
}

object RowSizeBufferSqlErrorResync extends SqlErrorHandler {
  def guard(conn: Connection)(f: => Unit): Unit = {
    RowSizeBufferSqlErrorHandler.guard(conn, Some(classOf[ResyncSecondaryException]))(f)
  }
}

final abstract class RowSizeBufferSqlErrorHandler

object RowSizeBufferSqlErrorHandler {
  private val logger = Logger[RowSizeBufferSqlErrorHandler]

  /**
   * Catch index row size buffer exception and optionally rethrow another exception or just ignore it.
   */
  def guard[E <: Exception](conn: Connection, exceptionClass: Option[Class[E]])(f: => Unit): Unit = {
    val savepoint = Option(conn.setSavepoint())

    def resyncIfIndexRowSizeError(ex: PSQLException): Unit = {
      isIndexRowSizeError(ex) match {
        case Success(indexOpt) =>
          val index = indexOpt.getOrElse("*unknown*")
          val exAction = exceptionClass.map("rethrow " + _.getName).getOrElse("ignore")
          val msg = s"index row size buffer exceeded $index $exAction"
          logger.warn(msg)
          savepoint.foreach(conn.rollback)
          exceptionClass.foreach(exClass => throw exClass.getDeclaredConstructor(classOf[String]).newInstance(msg))
        case Failure(innerEx) =>
          logger.info("got some PSQL exception, trying to roll back...", innerEx)
          savepoint.foreach(conn.rollback)
          throw innerEx
      }
    }

    try {
      f
    } catch {
      case ex: PSQLException => resyncIfIndexRowSizeError(ex)
       // sql exceptions wrapped in invocation target exception or from a batch update
      case e @ (_ : InvocationTargetException | _ : BatchUpdateException) =>
        e.getCause match {
          case ex: PSQLException => resyncIfIndexRowSizeError(ex)
          case ex: Throwable => throw ex
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
          case _ => Failure(ex)
        }
      case _ => Failure(ex)
    }
  }

  private val IndexRowSizeError = """index row size (\d+) exceeds (?:btree version \d+ )?maximum (\d+) for index (.*)""".r

  private val IndexRowSizeErrorWoIndexName = """index row requires (\d+) bytes, maximum size is (\d+)""".r
}
