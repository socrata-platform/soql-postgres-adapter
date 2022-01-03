package com.socrata.pg.soql

import com.socrata.soql.typed.{Hint, Materialized}

object Hints {

  def shouldMaterialized(hints: Seq[Hint[_, _]]) = {
    hints.exists {
      case Materialized(_) => true
      case _ => false
    }
  }
}
