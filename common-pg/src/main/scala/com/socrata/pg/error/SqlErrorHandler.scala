package com.socrata.pg.error

import java.sql.{Statement, Connection}

trait SqlErrorHandler {

  def guard(stmt: Statement)(f: => Unit) {
    guard(stmt.getConnection)(f)
  }

  def guard(conn: Connection)(f: => Unit): Unit

}