package com.socrata.pg.store

object PGSecondaryUtil {
  def testInternalName: String = "test_dataset" + System.currentTimeMillis()
  val localeName = "us"
  val obfuscationKey = "key".getBytes
}
