package com.socrata.pg.store

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.{JsonReader, JsonReaderException, CompactJsonWriter}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.matcher._
import com.typesafe.scalalogging.Logger

import com.socrata.datacoordinator.secondary.Secondary

case class PGCookie(
  deferredRollup: Boolean
)

object PGCookie {
  private val log = Logger[PGCookie]

  val default = PGCookie(
    deferredRollup = false
  )

  // using matchers rather than Automatic codecs to allow easier
  // evolution of the cookie.  We'll mark fields as Optional in
  // the pattern, and generate a default as appropriate when
  // decoding if it's not present.
  private val deferredRollupVar = Variable[Boolean]()
  private val Pattern = PObject(
    "deferredRollup" -> POption(deferredRollupVar)
  )

  implicit val decode = new JsonDecode[PGCookie] {
    override def decode(v: JValue) =
      Pattern.matches(v).right.map { vars =>
        PGCookie(
          deferredRollup = deferredRollupVar.getOrElse(vars, default.deferredRollup)
        )
      }
  }

  implicit val encode = new JsonEncode[PGCookie] {
    override def encode(cookie: PGCookie) = {
      // destructuring to force a compiler error when a field is added
      val PGCookie(deferredRollup) = cookie
      Pattern.generate(
        deferredRollupVar := deferredRollup
      )
    }
  }

  private object JsonAST {
    def unapply(s: String): Option[JValue] = {
      try {
        Some(JsonReader.fromString(s))
      } catch {
        case e: JsonReaderException =>
          log.warn("Unparsable cookie", e)
          None
      }
    }
  }

  def decode(cookie: Secondary.Cookie) =
    cookie match {
      case Some(JsonAST(cookie)) =>
        JsonDecode.fromJValue[PGCookie](cookie) match {
          case Right(result) =>
            result
            case Left(err) =>
            log.warn("Undecodable cookie: {}", err.english)
            default
        }
      case None =>
        default
    }

  def encode(cookie: PGCookie): Secondary.Cookie =
    Some(CompactJsonWriter.toString(JsonEncode.toJValue(cookie)))
}
