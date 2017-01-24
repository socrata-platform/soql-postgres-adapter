package com.socrata.pg.store

import java.util.concurrent.atomic.AtomicInteger

object PGSecondaryUtil {
  private val counter = new AtomicInteger(0)

  def testInternalName: String = "test_dataset" + counter.incrementAndGet()
  val localeName = "us"
  val obfuscationKey = "key".getBytes
}
