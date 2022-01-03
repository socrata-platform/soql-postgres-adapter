package com.socrata.pg.server

class SoQLChainMaterializedTest extends SoQLChainTest {
  override implicit val materialized: Boolean = true
}
