package com.socrata.pg.server

import scala.collection.JavaConverters._

import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.x.discovery.{ServiceDiscovery, ServiceInstance}
import org.slf4j.LoggerFactory

import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server.ServerBroker
import com.socrata.http.server.curator.CuratorBroker

class QueryServerBroker(
  portMapper: Int => Int,
  discovery: ServiceDiscovery[AuxiliaryData],
  address: String,
  serviceName: String,
  baseLCI: LivenessCheckInfo
) extends ServerBroker {
  private val log = LoggerFactory.getLogger(classOf[QueryServerBroker])

  private val remappedLCI = new LivenessCheckInfo(portMapper(baseLCI.port), baseLCI.response)

  class WrappedCookie private[QueryServerBroker] (
    private[QueryServerBroker] val port: Int,
    private[QueryServerBroker] var instance: Option[ServiceInstance[AuxiliaryData]]
  )

  type Cookie = WrappedCookie

  private class TrueBroker(suffix: String) extends CuratorBroker[AuxiliaryData](
    discovery,
    address,
    serviceName + suffix,
    Some(new AuxiliaryData(Some(remappedLCI)))
  ) {
    override def register(port: Int) =
      super.register(portMapper(port))
  }

  private val baseBroker = new TrueBroker("")
  private val enabled = new AtomicBoolean(true)
  private val issuedCookies = new IdentityHashMap[WrappedCookie, Unit]

  override def register(port: Int): WrappedCookie = synchronized {
    val cookie = new WrappedCookie(
      port,
      if(enabled.get) Some(baseBroker.register(port)) else None
    )
    issuedCookies.put(cookie, ())
    cookie
  }

  override def deregister(cookie: WrappedCookie) = synchronized {
    issuedCookies.remove(cookie)
    cookie.instance.foreach(baseBroker.deregister _)
  }

  // returns the old value
  def allowRequests(enable: Boolean): Boolean = {
    if(enabled.get == enable) {
      enable
    } else {
      synchronized {
        if(enabled.getAndSet(enable) == enable) return enable

        if(!enable) {
          log.warn("Disabling registration")
        } else {
          log.warn("Re-enabling registration")
        }
        for(cookie <- issuedCookies.keySet.asScala) {
          if(enable) {
            cookie.instance = Some(baseBroker.register(cookie.port))
          } else {
            cookie.instance.foreach(baseBroker.deregister _)
            cookie.instance = None
          }
        }

        !enable
      }
    }
  }
}
