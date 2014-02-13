package com.socrata.pg.store.config

trait ConfigHelper {
  val root: String

  def k(s: String) = root + "." + s
}