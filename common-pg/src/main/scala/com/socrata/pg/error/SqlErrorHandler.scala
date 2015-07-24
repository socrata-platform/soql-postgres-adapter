package com.socrata.pg.error

import java.sql.{Connection, Statement}

trait SqlErrorHandler {
  def guard(stmt: Statement)(f: => Unit): Unit = guard(stmt.getConnection)(f)
  def guard(conn: Connection)(f: => Unit): Unit
}
