package com.socrata.pg.store

object BuildSchema extends App {
  DatabaseCreator("com.socrata.soql-server-pg.database")
}
