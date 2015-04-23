package com.socrata.pg.server

class SoQLMoneyFunctionsTest extends SoQLTest {
  test("where money = 'txt'") {
    compareSoqlResult("select make, name, price where price = '3000.00' order by price", "where-money-eq-txt.json")
  }
  test("where money > 'txt'") {
    compareSoqlResult("select make, name, price where price > '3250.00' order by price", "where-money-gt-txt.json")
  }
  test("where money >= 'txt'") {
    compareSoqlResult("select make, name, price where price >= '3250.00' order by price", "where-money-ge-txt.json")
  }
  test("where money < 'txt'") {
    compareSoqlResult("select make, name, price where price < '2650.00' order by price", "where-money-lt-txt.json")
  }
  test("where money <= 'txt'") {
    compareSoqlResult("select make, name, price where price <= '2650.00' order by price", "where-money-le-txt.json")
  }
}