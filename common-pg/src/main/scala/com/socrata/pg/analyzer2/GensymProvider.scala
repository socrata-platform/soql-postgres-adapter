package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._

trait GensymProvider {
  def gensym(): Doc[Nothing]
}
