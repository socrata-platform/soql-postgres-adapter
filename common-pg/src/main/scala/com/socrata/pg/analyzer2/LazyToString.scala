package com.socrata.pg.analyzer2

sealed abstract class LazyToString {
  def indent(n: Int) =
    LazyToString(this.toString.replaceAll("\n", "\n" + " " * n))

  override def equals(o: Any) =
    o match {
      case that: LazyToString => this.toString == that.toString
      case _ => false
    }

  override def hashCode = toString.hashCode
}

object LazyToString {
  def apply[T](x: => Any): LazyToString =
    new LazyToString {
      override lazy val toString = x.toString
    }
}
