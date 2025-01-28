package com.socrata.pg.server.analyzer2

import scala.concurrent.duration._

import java.util.Comparator
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean}
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.PriorityQueue
import java.sql.SQLException
import javax.sql.DataSource

import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

class BundledTimeoutManager(ds: DataSource) extends ProcessQuery.TimeoutManager with AutoCloseable {
  // This keeps track of the pids of all extant queries with timeouts.
  // The only relevant race is: if a request completes (i.e., the
  // TimeoutHandle is closed) we must _not_ issue a pg_cancel_backend
  // for the pid.

  // Locking discipline:
  //   * mutex lock may be taken standalone or while holding a handle lock
  //   * a handle lock may _not_ be taken while holding the main mutex.

  private val log = LoggerFactory.getLogger(classOf[BundledTimeoutManager])

  private val mutex = new Object

  private class BTHandle(val pid: Int, d: FiniteDuration) extends ProcessQuery.TimeoutHandle with IntrusivePriorityQueue.Mixin {
    type Priority = Long

    val duration = Some(d)
    val durationMS = d.toMillis

    // lastping is only accessed/mutated during initialization
    // or while holding the handle lock
    var lastPing = System.currentTimeMillis
    def nextPing = lastPing + durationMS

    // cancelled are only accessed/mutated while holding the handle
    // lock.
    var cancelled = false

    def ping(): Unit =
      this.synchronized {
        val now = System.currentTimeMillis
        if(now < lastPing + 1000) return

        mutex.synchronized {
          lastPing = System.currentTimeMillis
          Worker.queue.adjustPriority(this)(nextPing)
          // No need to wake the worker up because the first item in
          // the queue is either the same as it was or has a later
          // deadline.
        }
      }
  }

  private object BTHandle {
    implicit object BTHResource extends Resource[BTHandle] {
      private def isSQLException(outer: Throwable): Boolean = {
        var current = outer
        while(current != null) {
          if(current.isInstanceOf[SQLException]) return true
          Option(current.getSuppressed).foreach { suppressed =>
            if(suppressed.exists(isSQLException)) return true
          }
          current = current.getCause
        }
        false
      }

      override def closeAbnormally(handle: BTHandle, exception: Throwable) = {
        if(!isSQLException(exception)) {
          if(unqueue(handle)) {
            cancel(handle)
          }
        }
      }

      override def close(handle: BTHandle) = {
        unqueue(handle)
      }

      private def unqueue(handle: BTHandle): Boolean = {
        mutex.synchronized {
          if(!Worker.queue.remove(handle)) return false
          // No need to wake the worker up because the first item in
          // the queue is either the same as it was or has a later
          // deadline.
        }

        handle.synchronized {
          handle.cancelled = true
        }

        true
      }
    }
  }

  private object Worker extends Thread {
    setName("Timeout manager")
    setDaemon(true)

    @volatile var stopping = false
    val alertSemaphore = new Semaphore(0)

    val queue = new IntrusivePriorityQueue[BTHandle](_ compareTo _)

    override def run() = {
      while(!stopping) {
        val timeout = mutex.synchronized {
          queue.peek.map(_._2)
        }.map(_ - System.currentTimeMillis).map(Math.max(_, 0L))

        timeout match {
          case Some(0) =>
            // don't want, just process
          case Some(timeout) =>
            log.info("Zzzzz... {}ms", timeout)
            alertSemaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS)
          case None =>
            log.info("Zzzzz...")
            alertSemaphore.acquire() // queue is empty, wait forever.
        }

        if(!stopping) processTask()
      }
    }

    private def processTask(): Unit = {
      val deadHandle = mutex.synchronized {
        queue.popIf { (entry, deadline) =>
          deadline <= System.currentTimeMillis
        }.getOrElse {
          return
        }
      }

      cancel(deadHandle)
    }
  }

  private def cancel(handle: BTHandle): Unit = {
    handle.synchronized {
      log.info("Cancelling process {}", handle.pid)
      for {
        conn <- managed(ds.getConnection())
          .and(_.setAutoCommit(true))
        stmt <- managed(conn.prepareStatement("SELECT pg_cancel_backend(?)"))
          .and(_.setInt(1, handle.pid))
        rs <- managed(stmt.executeQuery())
      } {}
      handle.cancelled = true
    }
  }

  def start() = Worker.start()

  override def close() = {
    Worker.stopping = true
    Worker.alertSemaphore.release()
    Worker.join()
  }

  def register(pid: Int, timeout: FiniteDuration, rs: ResourceScope): ProcessQuery.TimeoutHandle = {
    val handle = rs.open(new BTHandle(pid, timeout + 10.seconds))
    mutex.synchronized {
      Worker.queue.insert(handle)(handle.nextPing)
      Worker.alertSemaphore.release()
    }
    handle
  }
}
