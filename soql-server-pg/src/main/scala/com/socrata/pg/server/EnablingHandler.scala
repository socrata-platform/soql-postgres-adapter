package com.socrata.pg.server

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Semaphore, TimeUnit}

import org.slf4j.LoggerFactory

import com.socrata.http.server.{HttpRequest, HttpService}

class EnablingHandler(
  reregisterBelow: Int,
  deregisterAbove: Int,
  enable: Boolean => _,
  metricInterval: FiniteDuration,
  emit: Int => _,
  underlying: HttpService
) extends HttpService with AutoCloseable {
  private val inProgress = new AtomicInteger(0)
  private val max = new AtomicInteger(0)

  private val worker = new WorkerThread

  def start(): Unit = {
    worker.start()
  }

  override def close(): Unit = {
    worker.semaphore.release()
    worker.join()
  }

  def apply(req: HttpRequest) = { resp =>
    req.resourceScope.open(new EnablingHelper)
    underlying(req)(resp)
  }

  private class EnablingHelper extends AutoCloseable {
    trigger(inProgress.incrementAndGet())

    override def close() {
      trigger(inProgress.decrementAndGet())
    }

    def trigger(n: Int): Unit = {
      max.updateAndGet(_.max(n))

      if(n > deregisterAbove) enable(false)
      else if(n < reregisterBelow) enable(true)
    }
  }

  private class WorkerThread extends Thread {
    setDaemon(true)
    setName("Concurrency metricizer")

    private val log = LoggerFactory.getLogger(classOf[WorkerThread])
    private val metricIntervalNanos = metricInterval.toNanos

    val semaphore = new Semaphore(0)

    override def run(): Unit = {
      while(!semaphore.tryAcquire(metricIntervalNanos, TimeUnit.NANOSECONDS)) {
        try {
          emit(max.getAndSet(inProgress.get))
        } catch {
          case e: Exception =>
            log.warn("Unexpected exception while emitting metric", e)
        }
      }
    }
  }
}

object EnablingHandler {
  def apply(reregisterBelow: Int, deregisterAbove: Int, enable: Boolean => _, metricInterval: FiniteDuration, emit: Int => _, handler: HttpService): EnablingHandler =
    new EnablingHandler(reregisterBelow, deregisterAbove, enable, metricInterval, emit, handler)
}
