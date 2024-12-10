package com.socrata.pg.server


class SoQLLocationTest extends SoQLTest {
  test("select location") {
    compareSoqlResult("""select code, location where code='LOCATION'""", "select-location.json")
  }

  test("text to location and equality check") {
    compareSoqlResult(
      """SELECT code, location
        | WHERE location = '{
        |   latitude: 1.1,
        |   longitude: 2.2,
        |   human_address: ''{"address":"101 Main St", "city": "Seattle", "state": "WA", "zip": "98104"}''
        |   }'"""
        .stripMargin,
      "select-location.json")
  }

  test("text to location address only and equality check") {
    compareSoqlResult(
      """SELECT code, location
        | WHERE location = '{
        |   human_address: ''{"address":"101 Main St", "city": "Seattle", "state": "WA", "zip": "98104"}''
        |   }'"""
        .stripMargin,
      "select-location-address-only.json")
  }

  test("text to location lat lng only and equality check") {
    compareSoqlResult(
      """SELECT code, location WHERE location = '{ latitude: 1.1, longitude: 2.2 }'"""
        .stripMargin,
      "select-location-lat-lng-only.json")
  }

  test("location is not null") {
    compareSoqlResult("select code, location where location is not null order by code", "select-location-all.json")
  }

  test("location latitude longtitude") {
    compareSoqlResult(
      """select code,
              | location_latitude(location) as latitude,
              | location_longitude(location) as longitude
        | where location is not null""".stripMargin,
      "select-location-lat-lng.json")
  }

  test("location point") {
    compareSoqlResult("select code, location::point as point where location is not null order by code",
      "select-location-point.json")
  }

  test("location address") {
    compareSoqlResult("select code, location_human_address(location) as address where location is not null order by code",
      "select-location-address.json")
  }

  test("location within_circle") {
    compareSoqlResult(
      "select code, location where within_circle(location, 1.101, 2.201, 100000) order by code",
      "select-location-with-lat-lng.json")
  }

  test("location within_circle negative") {
    compareSoqlResult("select code, location where within_circle(location, 1.101, 102.201, 100000)", "empty.json")
  }

  test("location within_box") {
    compareSoqlResult(
      "select code, location where within_box(location, 1.09, 2.21, 1.11, 2.19) order by code",
      "select-location-with-lat-lng.json")
  }

  test("location within_box negative") {
    compareSoqlResult("select code, location where within_box(location, 1.09, 2.21, 1.11, 2.205)", "empty.json")
  }

  test("location latitude") {
    compareSoqlResult(
      "select code, location where location_latitude(location) = 1.1 order by code",
      "select-location-with-lat-lng.json")

    compareSoqlResult(
      "select code, location where location.latitude = 1.1 order by code",
      "select-location-with-lat-lng.json")
  }

  test("location longitude") {
    compareSoqlResult(
      "select code, location where location_longitude(location) = 2.2 order by code",
      "select-location-with-lat-lng.json")

    compareSoqlResult(
      "select code, location where location.longitude = 2.2 order by code",
      "select-location-with-lat-lng.json")
  }

  test("location address in where") {
    compareSoqlResult(
      """select code, location where location_human_address(location) =
        |'{"address":"101 Main St", "city": "Seattle", "state": "WA", "zip": "98104"}'""".stripMargin,
      "select-location-with-address.json")

    compareSoqlResult(
      """select code, location where location.human_address =
        |'{"address":"101 Main St", "city": "Seattle", "state": "WA", "zip": "98104"}'""".stripMargin,
      "select-location-with-address.json")
  }

  test("location constructor") {
    compareSoqlResult(
      "SELECT code, location('point (2.2 1.1)'::point, '101 Main St', 'Seattle', 'WA', '98104') as location WHERE code = 'LOCATION'",
      "select-location-ctor.json")
  }

  test("location constructor from columns") {
    compareSoqlResult(
      "SELECT code, location('point (2.2 1.1)'::point, url.description, code, 'WA', '98104') as location WHERE code = 'LOCATION'",
      "select-location-ctor-columns.json")
  }
}
