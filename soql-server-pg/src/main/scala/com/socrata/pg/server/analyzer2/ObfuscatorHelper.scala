package com.socrata.pg.server.analyzer2

import java.sql.Connection

import com.rojoma.simplearm.v2._

import com.socrata.soql.analyzer2.MetaTypes
import com.socrata.soql.environment.Provenance
import com.socrata.soql.sqlizer.{MetaTypesExt, Rep}

import com.socrata.pg.store.SqlUtils

object ObfuscatorHelper {
  def generateSoqlObfuscate(conn: Connection, obfuscators: Map[Provenance, Array[Byte]]): String = {
    def mkStringLiteral(s: String): String = "'" + SqlUtils.escapeString(conn, s) + "'"
    def mkTextLiteral(s: String): String = "text " + mkStringLiteral(s)
    def mkByteaLiteral(bs: Array[Byte]): String = "bytea " + mkStringLiteral(bs.iterator.map { b => "%02x".format(b & 0xff) }.mkString("\\x", "", ""))

    val preppedObfuscators =
      for {
        stmt <- managed(conn.prepareStatement(obfuscators.iterator.map { _ => "(?, make_obfuscator(?))" }.mkString("VALUES ", ",", ""))).
          and { stmt =>
            for(((prov, key), idx) <- obfuscators.iterator.zipWithIndex) {
              stmt.setString(idx * 2 + 1, prov.value)
              stmt.setBytes(idx * 2 + 2, key)
            }
          }
        rs <- managed(stmt.executeQuery())
      } {
        val result = Map.newBuilder[String, Array[Byte]]
        while(rs.next()) {
          result += rs.getString(1) -> rs.getBytes(2)
        }
        result.result()
      }

    val sqlPrefix =
      """CREATE OR REPLACE FUNCTION pg_temp.soql_obfuscate(provenance text) RETURNS bytea AS $$
          SELECT CASE provenance
              """

    val sqlSuffix = """
          END;
          $$ LANGUAGE sql
          IMMUTABLE
          STRICT
          PARALLEL SAFE"""

    preppedObfuscators.iterator.map { case (prov, preppedObfuscator) =>
      s"""WHEN ${mkTextLiteral(prov)} THEN ${mkByteaLiteral(preppedObfuscator)}"""
    }.mkString(sqlPrefix, "\n", sqlSuffix)
  }

  def setupObfuscators(conn: Connection, obfuscators: Map[Provenance, Array[Byte]]): Unit = {
    if(obfuscators.isEmpty) return

    val sql = generateSoqlObfuscate(conn, obfuscators);

    using(conn.createStatement()) { stmt =>
      stmt.execute(sql)
    }
  }
}
