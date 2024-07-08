package com.socrata.pg.server.analyzer2

import scala.concurrent.duration._
import java.{time => jt}

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.rojoma.json.v3.util.time.ISO8601.codec._

import com.socrata.soql.util.{SoQLErrorEncode, SoQLErrorDecode, SoQLErrorCodec, EncodedError}

sealed abstract class PostgresSoQLError

object PostgresSoQLError {
  case class RequestTimedOut(timeout: Option[Duration]) extends PostgresSoQLError
  object RequestTimedOut {
    private val tag = "soql.pg.timeout"

    case class Fields(timeout: Option[jt.Duration])
    object Fields {
      implicit val jCodec = AutomaticJsonCodecBuilder[Fields]
    }

    implicit def encode =
      new SoQLErrorEncode[RequestTimedOut] {
        override val code = tag

        override def encode(err: RequestTimedOut) =
          result(
            Fields(err.timeout.map { d => jt.Duration.ofMillis(d.toMillis) }),
            "Request timed out"
          )
      }

    implicit def decode =
      new SoQLErrorDecode[RequestTimedOut] {
        override val code = tag

        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
          } yield {
            RequestTimedOut(fields.timeout.map { d => d.toMillis.milliseconds })
          }
      }
  }

  case class QueryError(message: String) extends PostgresSoQLError
  object QueryError {
    private val tag = "soql.pg.query-error"

    case class Fields(message: String)
    object Fields {
      implicit val jCodec = AutomaticJsonCodecBuilder[Fields]
    }

    implicit def encode =
      new SoQLErrorEncode[QueryError] {
        override val code = tag

        override def encode(err: QueryError) =
          result(
            Fields(err.message),
            "Error running query"
          )
      }

    implicit def decode =
      new SoQLErrorDecode[QueryError] {
        override val code = tag

        def decode(v: EncodedError) =
          for {
            fields <- data[Fields](v)
          } yield {
            QueryError(fields.message)
          }
      }
  }

  def errorCodecs[T >: PostgresSoQLError <: AnyRef](
    codecs: SoQLErrorCodec.ErrorCodecs[T] = new SoQLErrorCodec.ErrorCodecs[T]
  ) = {
    codecs
      .branch[RequestTimedOut]
      .branch[QueryError]
  }
}
