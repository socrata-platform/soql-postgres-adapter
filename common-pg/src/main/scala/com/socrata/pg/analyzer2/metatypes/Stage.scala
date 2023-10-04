package com.socrata.pg.analyzer2.metatypes

import com.rojoma.json.v3.util.WrapperJsonCodec

case class Stage(underlying: String)
object Stage {
  implicit val jcodec = WrapperJsonCodec[Stage](Stage(_), _.underlying)

  implicit object stageOrdering extends Ordering[Stage] {
    def compare(a: Stage, b: Stage) = {
      a.underlying.compare(b.underlying)
    }
  }
}
