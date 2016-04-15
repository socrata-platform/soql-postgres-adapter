package com.socrata.pg.server


class SoQLUrlTest extends SoQLTest {
  test("select url") {
    compareSoqlResult("""select code, url where code='LOCATION'""", "select-url.json")
  }

  test("text to url and equality check") {
    compareSoqlResult(
      """SELECT code, url WHERE url = '<a href="http://www.socrata.com">Home Site</a>'""",
      "select-url.json")
  }

  test("text to url description only and equality check") {
    compareSoqlResult("""SELECT code, url WHERE url = '{ "description": "Home Site"}'""", "select-url-description.json")
  }

  test("text to url url only and equality check") {
    compareSoqlResult(
      "SELECT code, url WHERE url = 'http://www.socrata.com'",
      "select-url-url.json")
  }

  test("text to url (json) and equality check") {
    compareSoqlResult(
      """SELECT code, url WHERE url = '{ url: "http://www.socrata.com", description : "Home Site" }'""",
      "select-url.json")
  }

  test("url description is null") {
    compareSoqlResult(
      "SELECT code, url, url.url as url_url WHERE url is not null and url.description is null",
      "where-url_description-isnull.json")
  }

  test("url url is null") {
    compareSoqlResult(
      "SELECT code, url, url.description as url_description WHERE url is not null and url.url is null",
      "where-url_url-isnull.json")
  }

  test("url.description") {
    compareSoqlResult(
      "select code, url where url.description = 'Home Site' and code='LOCATION'",
      "select-url.json")
  }

  test("url.url") {
    compareSoqlResult(
      "select code, url where url.url = 'http://www.socrata.com' and code='LOCATION'",
      "select-url.json")
  }

  test("url constructor") {
    compareSoqlResult(
      "SELECT code, url('http://www.socrata.com', 'Home Site') as url WHERE code = 'LOCATION'",
      "select-url.json")
  }
}
