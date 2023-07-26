package com.socrata.pg.store

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.{Semaphore, TimeUnit}

import com.typesafe.scalalogging.Logger

final class WarnAfter(duration: FiniteDuration, log: Logger, msg: String) extends Thread {
  setName("lock timeout for " + Thread.currentThread.getName)
  setDaemon(true)

  val semaphore = new Semaphore(0)

  override def run() {
    if(!semaphore.tryAcquire(duration.toMillis, TimeUnit.MILLISECONDS)) {
      log.warn(msg)
    }
  }

  def completed() {
    semaphore.release()
    join()
  }
}
