package com.socrata.pg.server

import java.util.concurrent.atomic.AtomicInteger
import com.socrata.http.server.{HttpRequest, HttpService}

class EnablingHandler(limit: Int, enable: Boolean => _, underlying: HttpService) extends HttpService {
  private val inProgress = new AtomicInteger(0)

  def apply(req: HttpRequest) = { resp =>
    req.resourceScope.open(new EnablingHelper)
    underlying(req)(resp)
  }

  private class EnablingHelper extends AutoCloseable {
    enable(inProgress.incrementAndGet() < limit)

    override def close() {
      enable(inProgress.decrementAndGet() < limit)
    }
  }
}

object EnablingHandler {
  def apply(limit: Int, enable: Boolean => _, handler: HttpService): EnablingHandler =
    new EnablingHandler(limit, enable, handler)
}
