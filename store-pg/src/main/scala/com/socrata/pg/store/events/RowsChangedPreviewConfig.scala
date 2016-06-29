package com.socrata.pg.store.events

trait RowsChangedPreviewConfig {
  def minRows(columnCount: Int): Long
  def fractionOf(columnCount: Int): Double
}

object RowsChangedPreviewConfig {
  object Default extends RowsChangedPreviewConfig {
    override def minRows(columnCount: Int): Long = 100000
    override def fractionOf(columnCount: Int): Double = 0.33333
  }
}
