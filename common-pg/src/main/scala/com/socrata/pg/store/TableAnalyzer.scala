package com.socrata.pg.store

import com.socrata.datacoordinator.truth.metadata.CopyInfo

trait TableAnalyzer {
  def analyze(copy: CopyInfo): Unit
}
