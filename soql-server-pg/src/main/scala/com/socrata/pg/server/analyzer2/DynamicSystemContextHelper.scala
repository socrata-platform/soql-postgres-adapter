package com.socrata.pg.server.analyzer2

import java.sql.Connection

import com.rojoma.simplearm.v2._

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.environment.Provenance
import com.socrata.soql.sqlizer.{MetaTypesExt, Rep}

import com.socrata.pg.store.SqlUtils

object DynamicSystemContextHelper {
  def generateDynamicSystemContext(conn: Connection, ctx: Map[String, String]): String = {
    def mkStringLiteral(s: String): String = "'" + SqlUtils.escapeString(conn, s) + "'"
    def mkTextLiteral(s: String): String = "text " + mkStringLiteral(s)

    val body =
      if(ctx.isEmpty) {
        // We can have an empty dynamic context; this happens if a
        // user calls get_context with a non-literal parameter on a
        // dataset where core provides no context.  In that case, the
        // only answer can be NULL.
        "  SELECT null :: text"
      } else {
        ctx.iterator.map { case (key, value) =>
          s"""    WHEN ${mkTextLiteral(key)} THEN ${mkTextLiteral(value)}"""
        }.mkString("  SELECT CASE key\n", "\n", "\n  END")
      }

    val header = """CREATE OR REPLACE FUNCTION pg_temp.dynamic_system_context(key text) RETURNS text AS $$"""
    val footer = """$$ LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE"""

    s"$header\n$body\n$footer"
  }

  def setupDynamicSystemContext(conn: Connection, ctx: Map[String, String]): Unit = {
    val sql = generateDynamicSystemContext(conn, ctx);
    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }
}
