package com.socrata.pg.query

import com.rojoma.simplearm.v2.Managed
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.id.ColumnId
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.http.server.util.EntityTag
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.types.{SoQLType, SoQLValue}
import org.joda.time.DateTime

import java.sql.SQLException
import scala.concurrent.duration.Duration

sealed abstract class QueryResult

object QueryResult {

  case class NotModified(etags: Seq[EntityTag]) extends QueryResult
  case object PreconditionFailed extends QueryResult
  case class RequestTimedOut(timeout: Option[Duration]) extends QueryResult
  case class QueryError(description: String) extends QueryResult
  class QueryRuntimeError(val error: SQLException) extends QueryError(error.getMessage)
  object QueryRuntimeError {
    val validErrorCodes = Set(
      "22003",    // value overflows numeric format, 1000000000000 ^ 1000000000000000000
      "22012",    // divide by zero, 1/0
      "22008",    // timestamp out of range, timestamp - 'P500000Y'
      "22023",    // time zone not recognized
      "22P02",    // invalid input syntax for type boolean|numeric:, 'tr2ue'::boolean 'tr2ue'::number
      "XX000"     // invalid geometry, POINT(1,2)::point -- no comma between lon lat, POLYGON((0 0,1 1,1 0))::polygon
    )

    def apply(error: SQLException, timeout: Option[Duration] = None): Option[QueryResult] = {
      error.getSQLState match {
        // ick, but user-canceled requests also fall under this code and those are fine
        case "57014" if "ERROR: canceling statement due to statement timeout".equals(error.getMessage) =>
          Some(RequestTimedOut(timeout))
        case state if validErrorCodes.contains(state) =>
          Some(new QueryRuntimeError(error))
        case _ =>
          None
      }
    }
  }
  case class Success(
                      qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
                      copyNumber: Long,
                      dataVersion: Long,
                      results: Managed[CloseableIterator[Row[SoQLValue]] with RowCount],
                      etag: EntityTag,
                      lastModified: DateTime
                    ) extends QueryResult
  case class InfoSuccess(
                          copyNumber: Long,
                          dataVersion: Long,
                          explainBlob: ExplainInfo
                        ) extends QueryResult
}
