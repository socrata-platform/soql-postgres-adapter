package com.socrata.pg.store

import java.sql.Connection

import org.postgresql.core.BaseConnection

object SqlUtils {
  /**
    * Escapes a string appropriately for use on the given connection.  Supports
    * postgres wrapped postgres connections that can be discovered via Connection.unwrap.
    */
  def escapeString(conn: Connection, in: String): String = {
    conn match {
      case c: BaseConnection => c.escapeString(in)
      case c: Connection if c.isWrapperFor(classOf[Connection]) => escapeString(c.unwrap(classOf[Connection]), in)
      case _ => throw new Exception("Unsupported connection class: " + conn.getClass)
    }
  }
}
