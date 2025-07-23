package com.socrata.pg.server.analyzer2

class Nanoseconds(val ns: Long) extends AnyVal {
  def toMilliseconds: Long = ns / 1000000L
}

class MonotonicTime private (private val ns: Long) extends AnyVal {
  def -(that: MonotonicTime) = new Nanoseconds(this.ns - that.ns)
  def elapsed() = MonotonicTime.now() - this
}

object MonotonicTime {
  def now() = new MonotonicTime(System.nanoTime())
}
