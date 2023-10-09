package com.socrata.pg.analyzer2

import org.joda.time.DateTime

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer.ExtraContext

class SoQLExtraContext extends ExtraContext[SoQLExtraContext.Result] {
  val now = DateTime.now()
  var nonliteralSystemContextLookupFound: Boolean = false
  var nowUsed = false

  override def finish() =
    SoQLExtraContext.Result(
      nonliteralSystemContextLookupFound,
      if(nowUsed) Some(now) else None
    )
}

object SoQLExtraContext {
  case class Result(nonliteralSystemContextLookupFound: Boolean, now: Option[DateTime])
}
