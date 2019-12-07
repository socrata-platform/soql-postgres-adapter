package com.socrata.pg.store

trait PGStoreTestBase {

  val dcInstance = "alpha"

  val projectClassLoader = classOf[PGStoreTestBase].getClassLoader

  val storeId = "pg"

  val projectDb = "store"
}
