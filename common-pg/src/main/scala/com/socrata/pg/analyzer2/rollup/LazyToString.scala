package com.socrata.pg.analyzer2.rollup

trait LazyToString {
  def asString: String
  override lazy val toString = asString

  override def equals(that: Any) =
    that match {
      case lts: LazyToString => this.toString == that.toString
      case _ => false
    }

  override def hashCode = toString.hashCode

  def indent(n: Int) = LazyToString(this) { self =>
    self.toString.replaceAll("\n", "\n" + " " * n)
  }
}

object LazyToString {
  def apply[T](t: T)(f: T => String): LazyToString =
    new LazyToString {
      def asString = f(t)
    }
}
