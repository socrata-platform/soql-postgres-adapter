package com.socrata.pg.server.config

import com.typesafe.config.Config

class AdvertisementConfig(config: Config, root: String) {
  private def k(field: String) = root + "." + field

  val basePath = config.getString(k("base-path"))
  val name = config.getString(k("name"))
  val address = config.getString(k("address"))
}
