package com.socrata.pg.config

trait ConfigHelper {
  val root: String

  def k(s: String) = root + "." + s
}