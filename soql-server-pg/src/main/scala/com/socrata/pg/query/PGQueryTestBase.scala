package com.socrata.pg.query

trait PGQueryTestBase {

  val dcInstance = "alpha"

  val projectClassLoader = classOf[PGQueryTestBase].getClassLoader

  val storeId = "pg"

  val projectDb = "query"
}
