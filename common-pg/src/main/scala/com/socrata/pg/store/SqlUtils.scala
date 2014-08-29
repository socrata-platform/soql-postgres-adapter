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
      case c: Connection if c.isWrapperFor(classOf[BaseConnection]) =>
        c.unwrap(classOf[BaseConnection]).escapeString(in)
      case _ => throw new RuntimeException("Unsupported connection class: " + conn.getClass)

    }
  }
}
