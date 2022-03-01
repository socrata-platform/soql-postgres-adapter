package com.socrata.pg.queryx

import java.sql.SQLException
import scala.concurrent.duration.Duration

sealed abstract class QueryError
class QueryRuntimeError(val error: SQLException) extends QueryError
class QueryTimeoutError(val timeout: Option[Duration]) extends QueryError

object QueryRuntimeError {
  val validErrorCodes = Set(
    "22003",    // value overflows numeric format, 1000000000000 ^ 1000000000000000000
    "22012",    // divide by zero, 1/0
    "22008",    // timestamp out of range, timestamp - 'P500000Y'
    "22023",    // time zone not recognized
    "22P02",    // invalid input syntax for type boolean|numeric:, 'tr2ue'::boolean 'tr2ue'::number
    "XX000"     // invalid geometry, POINT(1,2)::point -- no comma between lon lat, POLYGON((0 0,1 1,1 0))::polygon
  )

  def apply(error: SQLException, timeout: Option[Duration] = None): Option[QueryError] = {
    error.getSQLState match {
      // ick, but user-canceled requests also fall under this code and those are fine
      case "57014" if "ERROR: canceling statement due to statement timeout".equals(error.getMessage) =>
        Some(new QueryTimeoutError(timeout))
      case state if validErrorCodes.contains(state) =>
        Some(new QueryRuntimeError(error))
      case _ =>
        None
    }
  }
}