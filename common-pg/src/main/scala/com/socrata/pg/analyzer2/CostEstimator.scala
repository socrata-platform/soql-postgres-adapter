package com.socrata.pg.analyzer2

import java.sql.Connection

import com.rojoma.json.v3.ast.JNumber
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.simplearm.v2._
import org.slf4j.LoggerFactory

final abstract class CostEstimator

object CostEstimator {
  private val log = LoggerFactory.getLogger(classOf[CostEstimator])

  def apply(conn: Connection, sql: String): Double = {
    log.debug("Considering {}", sql)
    for {
      stmt <- managed(conn.createStatement())
      rs <- managed(stmt.executeQuery("EXPLAIN (FORMAT JSON) " + sql))
    } {
      if(!rs.next()) {
        throw new Exception("EXPLAIN should have returned a single result?");
      }

      val result = JsonReader.fromString(rs.getString(1)).dyn(0).Plan("Total Cost").! match {
        case n: JNumber => n.toDouble
        case _ => throw new Exception("Total Cost should have been a number")
      }

      log.debug("Estimated cost: {}", result)

      result
    }
  }
}
