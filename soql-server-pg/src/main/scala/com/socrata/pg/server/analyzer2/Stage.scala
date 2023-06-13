package com.socrata.pg.server.analyzer2

import com.rojoma.json.v3.util.WrapperJsonCodec

case class Stage(underlying: String)
object Stage {
  implicit object hashable extends Hashable[Stage] {
    override def hash(hasher: Hasher, value: Stage) = hasher.hashString(value.underlying)
    override def isString = true
  }

  implicit val jcodec = WrapperJsonCodec[Stage](Stage(_), _.underlying)

  implicit object stageOrdering extends Ordering[Stage] {
    def compare(a: Stage, b: Stage) = {
      a.underlying.compare(b.underlying)
    }
  }
}
