package com.socrata.pg.soql

import SqlizerTest._
import com.socrata.soql.exceptions.{TypecheckException, TypeMismatch}

// scalastyle:off null
class SqlizerPhoneTest extends SqlizerTest {

  // test("non-existing phone type does not convert") {
  //   val soql = "select * where phone = 'NonexistingType: 4251234567'"
  //   val ex = intercept[TypecheckException] {
  //     sqlize(soql, CaseSensitive)
  //   }
  //   ex.getMessage should be("Cannot pass a value of type `text' to function `op$=':\nselect * where phone = 'NonexistingType: 4251234567'\n                       ^")
  // }
}
