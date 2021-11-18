package com.socrata.pg.soql

import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLFloatingTimestamp, SoQLInterval, SoQLType}

sealed trait SqlizeError extends Exception {
  val value: String

  override def getMessage(): String = {
    s"${this.getClass.getSimpleName} $value"
  }
}

case class InvalidInterval(value: String) extends SqlizeError
case class InvalidTimestamp(value: String) extends SqlizeError
case class InvalidTimezone(value: String) extends SqlizeError
class InvalidConversion(val value: String) extends SqlizeError
object InvalidConversion {
  def apply(soqlType: SoQLType, value: String): SqlizeError = {
    soqlType match {
      case SoQLFloatingTimestamp => InvalidTimestamp(value)
      case SoQLFixedTimestamp => InvalidTimestamp(value)
      case SoQLInterval => InvalidInterval(value)
      case _ => new InvalidConversion(value)
    }
  }
}
