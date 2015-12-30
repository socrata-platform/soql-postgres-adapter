package com.socrata.pg.server


class SoQLLocationTest extends SoQLTest {
  test("select location") {
    compareSoqlResult("""select code, location where code='LOCATION'""", "select-location.json")
  }

  test("location is not null") {
    compareSoqlResult("select code, location where location is not null", "select-location.json")
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
    compareSoqlResult("select code, location::point as point where location is not null",
      "select-location-point.json")
  }

  test("location address") {
    compareSoqlResult("select code, location_address(location) as address where location is not null",
      "select-location-address.json")
  }

  test("location within_circle") {
    compareSoqlResult("select code, location where within_circle(location, 1.101, 2.201, 100000)", "select-location.json")
  }

  test("location within_circle negative") {
    compareSoqlResult("select code, location where within_circle(location, 1.101, 102.201, 100000)", "empty.json")
  }

  test("location within_box") {
    compareSoqlResult("select code, location where within_box(location, 1.09, 2.21, 1.11, 2.19)", "select-location.json")
  }

  test("location within_box negative") {
    compareSoqlResult("select code, location where within_box(location, 1.09, 2.21, 1.11, 2.205)", "empty.json")
  }
}
