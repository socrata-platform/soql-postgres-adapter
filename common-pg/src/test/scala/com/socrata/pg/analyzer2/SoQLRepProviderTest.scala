package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, MustMatchers}
import org.joda.time.{DateTime, DateTimeZone, LocalDateTime, LocalDate, LocalTime}

class SoQLRepProviderTest extends FunSuite with MustMatchers {
  test("fixed timestamp - no fraction") {
    SoQLRepProvider.pgCsvFixedFormat.parseDateTime("2001-02-03 14:10:33-01") must equal (
      // 15 because 1 hour off from UTC
      new DateTime(2001, 2, 3, 15, 10, 33, DateTimeZone.UTC)
    )
  }

  test("fixed timestamp - milli-scale fraction") {
    SoQLRepProvider.pgCsvFixedFormat.parseDateTime("2001-02-03 14:10:33.567-01") must equal (
      // 15 because 1 hour off from UTC
      new DateTime(2001, 2, 3, 15, 10, 33, 567, DateTimeZone.UTC)
    )
  }

  test("fixed timestamp - micro-scale fraction") {
    SoQLRepProvider.pgCsvFixedFormat.parseDateTime("2001-02-03 14:10:33.567891-01") must equal (
      // 15 because 1 hour off from UTC
      new DateTime(2001, 2, 3, 15, 10, 33, 567, DateTimeZone.UTC)
    )
  }

  test("floating timestamp - no fraction") {
    SoQLRepProvider.pgCsvFloatingFormat.parseLocalDateTime("2001-02-03 14:10:33") must equal (
      new LocalDateTime(2001, 2, 3, 14, 10, 33)
    )
  }

  test("floating timestamp - milli-scale fraction") {
    SoQLRepProvider.pgCsvFloatingFormat.parseLocalDateTime("2001-02-03 14:10:33.567") must equal (
      new LocalDateTime(2001, 2, 3, 14, 10, 33, 567)
    )
  }

  test("floating timestamp - micro-scale fraction") {
    SoQLRepProvider.pgCsvFloatingFormat.parseLocalDateTime("2001-02-03 14:10:33.567891") must equal (
      new LocalDateTime(2001, 2, 3, 14, 10, 33, 567)
    )
  }

  test("date") {
    SoQLRepProvider.pgCsvDateFormat.parseLocalDate("2001-02-03") must equal (
      new LocalDate(2001, 2, 3)
    )
  }

  test("time - no fraction") {
    SoQLRepProvider.pgCsvTimeFormat.parseLocalTime("14:10:33") must equal (
      new LocalTime(14, 10, 33)
    )
  }

  test("time - milli-scale fraction") {
    SoQLRepProvider.pgCsvTimeFormat.parseLocalTime("14:10:33.567") must equal (
      new LocalTime(14, 10, 33, 567)
    )
  }

  test("time - micro-scale fraction") {
    SoQLRepProvider.pgCsvTimeFormat.parseLocalTime("14:10:33.567891") must equal (
      new LocalTime(14, 10, 33, 567)
    )
  }
}
