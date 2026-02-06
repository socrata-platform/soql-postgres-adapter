package com.socrata.pg.server

import java.util.concurrent.atomic.AtomicInteger
import com.socrata.http.server.{HttpRequest, HttpService}

class EnablingHandler(reregisterBelow: Int, deregisterAbove: Int, enable: Boolean => _, underlying: HttpService) extends HttpService {
  private val inProgress = new AtomicInteger(0)

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
      if(n > deregisterAbove) enable(false)
      else if(n < reregisterBelow) enable(true)
    }
  }
}

object EnablingHandler {
  def apply(reregisterBelow: Int, deregisterAbove: Int, enable: Boolean => _, handler: HttpService): EnablingHandler =
    new EnablingHandler(reregisterBelow, deregisterAbove, enable, handler)
}
