package com.socrata.pg.server.config

import com.typesafe.scalalogging.Logger

object DynamicPortMap {
  private val logger = Logger[DynamicPortMap]
}

trait DynamicPortMap {
  import DynamicPortMap.logger

  private val intRx = "(\\d+)".r

  def hostPort(port: Int): Int = {
    Option(System.getenv (s"PORT_$port")) match {
      case Some (intRx (hostPort) ) =>
        logger.info ("host_port: {} -> container_port: {}", hostPort, port.toString)
        hostPort.toInt
      case _ => port
    }
  }
}
