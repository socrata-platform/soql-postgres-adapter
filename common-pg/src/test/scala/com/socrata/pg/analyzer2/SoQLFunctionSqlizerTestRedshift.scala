package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, Matchers}
import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.pg.store.PGSecondaryUniverseTestBase
import com.socrata.soql.functions._

import com.typesafe.config.ConfigFactory
import com.socrata.pg.config.StoreConfig
import com.socrata.datacoordinator.common._
import com.socrata.datacoordinator.secondary.DatasetInfo

object SoQLFunctionSqlizerTestRedshift {
  final abstract class TestMT extends MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Int
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }
}

class SoQLFunctionSqlizerTestRedshift extends FunSuite with Matchers with SqlizerUniverse[SoQLFunctionSqlizerTest.TestMT] with PGSecondaryUniverseTestBase {

  override val config = {
    val config = ConfigFactory.load()
    new StoreConfig(config.getConfig("com.socrata.pg.store.secondary"), "")
  }

  type TestMT = SoQLFunctionSqlizerTest.TestMT

  val sqlizer = new Sqlizer {
    override val namespace = new SqlNamespaces {
      def databaseColumnBase(dcn: DatabaseColumnName) = {
        val DatabaseColumnName(n) = dcn
        Doc(n)
      }
      def databaseTableName(dcn: DatabaseTableName) = {
        val DatabaseTableName(n) = dcn
        Doc(n)
      }
    }

    val cryptProvider = obfuscation.CryptProvider.zeros

    override val repFor = new SoQLRepProviderRedshift[TestMT](_ => Some(cryptProvider), namespace, Map.empty, Map.empty) {
      def mkStringLiteral(string: String) = Doc(s"'$string'")
    }

    override val funcallSqlizer = new SoQLFunctionSqlizerRedshift

    override val rewriteSearch = new SoQLRewriteSearch(searchBeforeQuery = true)

    override val systemContext = Map("hello" -> "world")
  }

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[TestMT](items.toMap)
  val analyzer = new SoQLAnalyzer[TestMT](SoQLTypeInfo, SoQLFunctionInfo)
  def analyze(soqlexpr: String): String = {
    val s = analyzeStatement(s"SELECT ($soqlexpr)")
    val prefix = "SELECT "
    val suffix = " AS i1 FROM table1 AS x1"
    if(s.startsWith(prefix) && s.endsWith(suffix)) {
      s.dropRight(suffix.length).drop(prefix.length)
    } else {
      s
    }
  }
  def analyzeStatement(stmt: String, useSelectListReferences: Boolean = false) = {
    val tf = MockTableFinder[TestMT](
      (0, "table1") -> D(
        "text" -> SoQLText,
        "num" -> SoQLNumber,
        "url" -> SoQLUrl,
        "geom" -> SoQLPolygon
      )
    )

    val ft =
      tf.findTables(0, ResourceName("table1"), stmt, Map.empty) match {
        case Right(ft) => ft
        case Left(err) => fail("Bad query: " + err)
      }

    var analysis =
      analyzer(ft, UserParameters.empty) match {
        case Right(an) => an
        case Left(err) => fail("Bad query: " + err)
      }

    if(useSelectListReferences) analysis = analysis.useSelectListReferences

    val sql = sqlizer(analysis.statement).sql.layoutSingleLine.toString

    sql
  }

  test("foo") {
    println(analyzeStatement("select signed_magnitude_linear(12, 3)"))
  }

  test("is null works") {
    analyzeStatement("SELECT text, num WHERE text is null") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NULL""")
  }

  test("is not null works") {
    analyzeStatement("SELECT text, num WHERE text is not null") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) IS NOT NULL""")
  }

  test("not works") {
    analyzeStatement("SELECT text, num WHERE NOT text = 'two'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE NOT((x1.text) = (text 'two'))""")
  }

  test("between x and y works") {
    analyzeStatement("SELECT text, num WHERE num between 0 and 3") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))""")
  }

  test("not between x and y works") {
    analyzeStatement("SELECT text, num WHERE num not between 0 and 3") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) not between (0 :: decimal(30, 7)) and (3 :: decimal(30, 7))""")
  }

  test("in subset works") {
    analyzeStatement("SELECT text, num WHERE num in (1, 2, 3)") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))""")
  }

  test("in subset works case insensitively") {
    analyzeStatement("SELECT text, num WHERE caseless_one_of(text, 'one', 'two', 'three')") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))""")
  }

  test("not in works") {
    analyzeStatement("SELECT text, num WHERE num not in (1, 2, 3)") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) NOT IN (1 :: decimal(30, 7), 2 :: decimal(30, 7), 3 :: decimal(30, 7))""")
  }

  test("caseless not one of works") {
    analyzeStatement("SELECT text, num WHERE caseless_not_one_of(text, 'one', 'two', 'three')") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) NOT IN (upper(text 'one'), upper(text 'two'), upper(text 'three'))""")
  }

  test("equal = works with int") {
    analyzeStatement("SELECT text, num WHERE num = 1") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))""")
  }

  test("equal = works with text") {
    analyzeStatement("SELECT text, num WHERE text = 'TWO'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')""")
  }

  test("equal == works with text") {
    analyzeStatement("SELECT text, num WHERE text == 'TWO'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) = (text 'TWO')""")
  }

  test("equal == works with int") {
    analyzeStatement("SELECT text, num WHERE num == 1") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) = (1 :: decimal(30, 7))""")
  }

  test("caseless equal works") {
    analyzeStatement("SELECT text, num WHERE caseless_eq(text, 'TWO')") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (upper(text 'TWO'))""")
  }

  test("not equal <> works") {
    analyzeStatement("SELECT text, num WHERE num <> 2") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))""")
  }

  test("not equal != works") {
    analyzeStatement("SELECT text, num WHERE num != 2") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <> (2 :: decimal(30, 7))""")
  }

  test("caseless not equal works") {
    analyzeStatement("SELECT text, num WHERE caseless_ne(text, 'TWO')") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) <> (upper(text 'TWO'))""")
  }

  test("and works") {
    analyzeStatement("SELECT text, num WHERE num == 1 and text == 'one'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) = (1 :: decimal(30, 7))) AND ((x1.text) = (text 'one'))""")
  }

  test("or works") {
    analyzeStatement("SELECT text, num WHERE num < 5 or text == 'two'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) < (5 :: decimal(30, 7))) OR ((x1.text) = (text 'two'))""")
  }

    test("less than works") {
      analyzeStatement("SELECT text, num WHERE num < 2.0001") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) < (2.0001 :: decimal(30, 7))""")
  }

  test("les than or equals works") {
    analyzeStatement("SELECT text, num WHERE num <= 2.1") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) <= (2.1 :: decimal(30, 7))""")
  }

  test("greater than works") {
    analyzeStatement("SELECT text, num WHERE num > 0.9") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) > (0.9 :: decimal(30, 7))""")
  }

  test("greater than or equals works") {
    analyzeStatement("SELECT text, num WHERE num >= 2") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.num) >= (2 :: decimal(30, 7))""")
  }

  test("least works") {
    analyzeStatement("SELECT LEAST(1, 1.1, 0.9)") should equal("""SELECT least(1 :: decimal(30, 7), 1.1 :: decimal(30, 7), 0.9 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("greatest works") {
    analyzeStatement("SELECT GREATEST(0.9, 1, 1.1)") should equal("""SELECT greatest(0.9 :: decimal(30, 7), 1 :: decimal(30, 7), 1.1 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("like works with percent") {
    analyzeStatement("SELECT text, num WHERE text LIKE 't%'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't%')""")
  }

  test("like works with underscore") {
    analyzeStatement("SELECT text, num WHERE text LIKE 't__'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) LIKE (text 't__')""")
  }

  test("not like works with percent") {
    analyzeStatement("SELECT text, num WHERE text NOT LIKE 't%'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't%')""")
  }

  test("not like works with underscore") {
    analyzeStatement("SELECT text, num WHERE text NOT LIKE 't__'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (x1.text) NOT LIKE (text 't__')""")
  }

  test("concat || works"){
    analyzeStatement("SELECT text || num") should equal("""SELECT (x1.text) || (x1.num) AS i1 FROM table1 AS x1""")
  }

  test("lower() works") {
    analyzeStatement("SELECT lower(text), num where lower(text) == 'two'") should equal("""SELECT lower(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (lower(x1.text)) = (text 'two')""")
  }

  test("upper() works") {
    analyzeStatement("SELECT upper(text), num where upper(text) == 'TWO'") should equal("""SELECT upper(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1 WHERE (upper(x1.text)) = (text 'TWO')""")
  }

  test("length() works") {
    analyzeStatement("SELECT length(text), num") should equal("""SELECT length(x1.text) AS i1, x1.num AS i2 FROM table1 AS x1""")
  }

  test("replace() works") {
    analyzeStatement("SELECT num ,replace(text, 'o', 'z')") should equal("""SELECT x1.num AS i1, replace(x1.text, text 'o', text 'z') AS i2 FROM table1 AS x1""")
  }

  test("trimming on both ends works") {
    analyzeStatement("SELECT trim('   abc   ')") should equal("""SELECT trim(text '   abc   ') AS i1 FROM table1 AS x1""")
  }

  test("trimming on leading spaces works") {
    analyzeStatement("SELECT trim_leading('   abc   ')") should equal("""SELECT ltrim(text '   abc   ') AS i1 FROM table1 AS x1""")
  }

  test("trimming on trailing spaces works") {
    analyzeStatement("SELECT trim_trailing('   abc   ')") should equal("""SELECT rtrim(text '   abc   ') AS i1 FROM table1 AS x1""")
  }

  test("left_pad works") {
    analyzeStatement("SELECT left_pad(text, 10, 'a'), num") should equal("""SELECT lpad(x1.text, 10 :: decimal(30, 7) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1""")
  }

  test("right_pad works") {
    analyzeStatement("SELECT right_pad(text, 10, 'a'), num") should equal("""SELECT rpad(x1.text, 10 :: decimal(30, 7) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1""")
  }

  test("chr() works") {
    analyzeStatement("SELECT chr(50.2)") should equal("""SELECT chr(50.2 :: decimal(30, 7) :: int) AS i1 FROM table1 AS x1""")
  }

  test("substring(characters, start_index base 1) works"){
    analyzeStatement("SELECT substring('abcdefghijk', 3)") should equal("""SELECT substring(text 'abcdefghijk', 3 :: decimal(30, 7) :: int) AS i1 FROM table1 AS x1""")
  }

  test("substring(characters, start_index base 1, length) works") {
    analyzeStatement("SELECT substring('abcdefghijk', 3, 4)") should equal("""SELECT substring(text 'abcdefghijk', 3 :: decimal(30, 7) :: int, 4 :: decimal(30, 7) :: int) AS i1 FROM table1 AS x1""")
  }

  test("split_part works") {
    analyzeStatement("SELECT split_part(text, '.', 3)") should equal("""SELECT split_part(x1.text, text '.', 3 :: decimal(30, 7) :: int) AS i1 FROM table1 AS x1""")
  }

  test("uniary minus works") {
    analyzeStatement("SELECT text, - num") should equal("""SELECT x1.text AS i1, -(x1.num) AS i2 FROM table1 AS x1""")
  }

  test("uniary plus works") {
    analyzeStatement("SELECT text, + num") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1""")
  }

  test("binary minus works") {
    analyzeStatement("SELECT text, num - 1") should equal("""SELECT x1.text AS i1, (x1.num) - (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("binary plus works") {
    analyzeStatement("SELECT text, num + 1") should equal("""SELECT x1.text AS i1, (x1.num) + (1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("num times num works") {
    analyzeStatement("SELECT text, num * 2") should equal("""SELECT x1.text AS i1, (x1.num) * (2 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("doube times double works") {
    analyzeStatement("SELECT 5.4567 * 9.94837") should equal("""SELECT (5.4567 :: decimal(30, 7)) * (9.94837 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("division works") {
    analyzeStatement("SELECT 6.4354 / 3.423") should equal("""SELECT (6.4354 :: decimal(30, 7)) / (3.423 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("exponents work") {
    analyzeStatement("SELECT 7.4234 ^ 2") should equal("""SELECT (7.4234 :: decimal(30, 7)) ^ (2 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("modulo works") {
    analyzeStatement("SELECT 6.435 % 3.432") should equal("""SELECT (6.435 :: decimal(30, 7)) % (3.432 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("ln works") {
    analyzeStatement("SELECT ln(16)") should equal("""SELECT ln(16 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("absolute works") {
    analyzeStatement("SELECT abs(-1.234)") should equal("""SELECT abs(-1.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("ceil() works") {
    analyzeStatement("SELECT ceil(4.234)") should equal("""SELECT ceil(4.234 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }

  test("floor works") {
    analyzeStatement("SELECT floor(9.89)") should equal("""SELECT floor(9.89 :: decimal(30, 7)) AS i1 FROM table1 AS x1""")
  }
}
