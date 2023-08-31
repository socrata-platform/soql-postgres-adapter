package com.socrata.pg.analyzer2.rollup

import com.socrata.soql.analyzer2._

trait HasLabelProvider {
  protected val labelProvider: LabelProvider
}
