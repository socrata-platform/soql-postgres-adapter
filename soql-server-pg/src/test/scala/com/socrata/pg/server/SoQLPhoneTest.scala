package com.socrata.pg.server


class SoQLPhoneTest extends SoQLTest {
  test("select phone") {
    compareSoqlResult("""select code, phone where code='LOCATION'""", "select-phone.json")
  }

  /**
   * Phone type is normalized and compare in normalized form.
   */
  test("text to phone and equality check") {
    compareSoqlResult(
      "SELECT code, phone WHERE phone = 'HoME: 4251234567'",
      "select-phone.json")
  }

  /**
   * phone number is null.  To ignore phone number, phone.type = 'something' should be used.
   */
  test("text to phone type only and equality check") {
    compareSoqlResult(
      "SELECT code, phone WHERE phone = 'Home'",
      "select-phone-type.json")
  }

  /**
   * phone type is null.  To ignore phone type, phone.number = 'something' should be used.
   */
  test("text to phone number only and equality check") {
    compareSoqlResult(
      "SELECT code, phone WHERE phone = '4251234567'",
      "select-phone-number.json")
  }

  test("text to phone (json) and equality check") {
    compareSoqlResult(
      """SELECT code, phone WHERE phone = '{ phone_type: "Home", phone_number : "4251234567" }'""",
      "select-phone.json")
  }

  test("phone type is null") {
    compareSoqlResult(
      "SELECT code, phone, phone.phone_number as phone_number WHERE phone is not null and phone.phone_type is null",
      "where-phone_type-isnull.json")
  }

  test("phone number is null") {
    compareSoqlResult(
      "SELECT code, phone, phone.phone_type as phone_type WHERE phone is not null and phone.phone_number is null",
      "where-phone_number-isnull.json")
  }

  test("phone.phone_type") {
    compareSoqlResult(
      "select code, phone where phone.phone_type = 'Home' and code='LOCATION'",
      "select-phone.json")
  }

  test("phone.phone_number") {
    compareSoqlResult(
      "select code, phone where phone.phone_number = '4251234567' and code='LOCATION'",
      "select-phone.json")
  }
}
