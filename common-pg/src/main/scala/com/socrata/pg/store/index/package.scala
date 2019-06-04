package com.socrata.pg.store

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

package object index {
  implicit val codec = AutomaticJsonCodecBuilder[IndexDirectives]
}
