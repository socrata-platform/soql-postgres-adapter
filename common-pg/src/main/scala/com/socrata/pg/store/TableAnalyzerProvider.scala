package com.socrata.pg.store

trait TableAnalyzerProvider {
  def analyzer: TableAnalyzer
}
