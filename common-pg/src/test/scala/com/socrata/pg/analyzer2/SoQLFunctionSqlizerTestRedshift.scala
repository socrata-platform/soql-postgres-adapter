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
      def mkStringLiteral(s: String) = Doc(JString(s).toString)
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

  //TODO change the redshift sqlizer to produce only single quotes around string literals
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
    analyzeStatement("SELECT text, num WHERE num == 1 and text == 'one'") should equal("""SELECT x1.text AS i1, x1.num AS i2 FROM table1 AS x1 WHERE ((x1.num) = (1 :: numeric)) AND ((x1.text) = (text 'one'))""")
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
    println(analyzeStatement("SELECT lower(text), num where lower(text) == 'two'"))
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
    println(analyzeStatement("SELECT trim('a' from 'abc')"))
  }


  test("tst") {
    onlyRunIf(Redshift) {
      withPgu { n => ???}
    }
  }

  test("basic search") {
    analyzeStatement("SELECT 1 SEARCH 'hello'") should equal("""SELECT 1 :: numeric AS i1 FROM table1 AS x1 WHERE (to_tsvector('english', ((((coalesce(x1.text, text "")) || (text " ")) || (coalesce(x1.url_url, text ""))) || (text " ")) || (coalesce(x1.url_description, text "")))) @@ (plainto_tsquery('english', text "hello"))""");
  }

  test("search with where") {
    analyzeStatement("SELECT 1 where num > 5 SEARCH 'hello'") should equal("""SELECT 1 :: numeric AS i1 FROM table1 AS x1 WHERE ((to_tsvector('english', ((((coalesce(x1.text, text "")) || (text " ")) || (coalesce(x1.url_url, text ""))) || (text " ")) || (coalesce(x1.url_description, text "")))) @@ (plainto_tsquery('english', text "hello"))) AND ((x1.num) > (5 :: numeric))""");
  }

  test("subquery search") {
    analyzeStatement("SELECT text, url |> select 1 SEARCH 'hello'") should equal("""SELECT 1 :: numeric AS i3 FROM (SELECT x1.text AS i1, x1.url_url AS i2_url, x1.url_description AS i2_description FROM table1 AS x1) AS x2 WHERE (to_tsvector('english', ((((coalesce(x2.i1, text "")) || (text " ")) || (coalesce(x2.i2_url, text ""))) || (text " ")) || (coalesce(x2.i2_description, text "")))) @@ (plainto_tsquery('english', text "hello"))""")
  }

  test("subquery search - compressed") {
    // the "case" here just forces url to be compressed
    analyzeStatement("SELECT text, case when true then url end |> select 1 SEARCH 'hello'") should equal ("""SELECT 1 :: numeric AS i3 FROM (SELECT x1.text AS i1, soql_compress_compound(x1.url_url, x1.url_description) AS i2 FROM table1 AS x1) AS x2 WHERE (to_tsvector('english', ((((coalesce(x2.i1, text "")) || (text " ")) || (coalesce((x2.i2) ->> 0, text ""))) || (text " ")) || (coalesce((x2.i2) ->> 1, text "")))) @@ (plainto_tsquery('english', text "hello"))""")
  }

  test("all window functions are covered") {
    sqlizer.funcallSqlizer.windowedFunctionMap.keySet == SoQLFunctions.windowFunctions.toSet
  }

  test("lead/lag gets its int cast") {
    analyze("lead(text, 5) OVER ()") should equal ("lead(x1.text, (5 :: numeric) :: int) OVER ()")
  }

  test("convex_hull gets its buffer wrapper") {
    analyze("convex_hull(geom)") should equal ("st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0)))")
  }

  test("concave_hull gets its buffer wrapper") {
    analyze("concave_hull(geom,3.0)") should equal ("st_asbinary(st_multi(st_buffer(st_concavehull(x1.geom, 3.0 :: numeric), 0.0)))")
  }

  test("things get correctly de-select-list-referenced") {
    // The convex_hull(geom) gets de-referenced because of introducing
    // the st_asbinary call, but the text||text continues to be
    // referenced.
    analyzeStatement("select convex_hull(geom) as a, text||text as b group by a, b", useSelectListReferences = true) should equal ("SELECT st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0))) AS i1, (x1.text) || (x1.text) AS i2 FROM table1 AS x1 GROUP BY st_multi(st_buffer(st_convexhull(x1.geom), 0.0)), 2")
  }

  test("case - else") {
    analyze("case when 1=1 then 'hello' when 1=2 then 'world' else 'otherwise' end") should equal ("""CASE WHEN (1 :: numeric) = (1 :: numeric) THEN text "hello" WHEN (1 :: numeric) = (2 :: numeric) THEN text "world" ELSE text "otherwise" END""")
  }

  test("case - no else") {
    analyze("case when 1=1 then 'hello' when 1=2 then 'world' end") should equal ("""CASE WHEN (1 :: numeric) = (1 :: numeric) THEN text "hello" WHEN (1 :: numeric) = (2 :: numeric) THEN text "world" END""")
  }

  test("iif") {
    analyze("iif(1 = 1, 2, 3)") should equal ("CASE WHEN (1 :: numeric) = (1 :: numeric) THEN 2 :: numeric ELSE 3 :: numeric END")
  }

  test("uncased works") {
    analyze("caseless_eq(text,'bleh')") should equal ("""(upper(x1.text)) = (upper(text "bleh"))""")
  }

  test("count gets cast to numeric") {
    analyze("count(*)") should equal ("""(count(*)) :: numeric""")
    analyze("count(text)") should equal ("""(count(x1.text)) :: numeric""")
    analyze("count(distinct text)") should equal ("""(count(DISTINCT x1.text)) :: numeric""")
  }

  test("get_context known literal") {
    analyze("get_context('hello')") should equal ("""text "world"""")
  }

  test("get_context unknown literal") {
    analyze("get_context('goodbye')") should equal ("""null :: text""")
  }

  test("get_context non-literal") {
    analyze("get_context(text)") should equal ("""current_setting('socrata_system.a' || md5(x1.text), true)""")
  }

  test("url(x, y).url == x") {
    analyze("url('x','y').url") should equal ("""text "x"""")
  }

  test("url(x, y).description == y") {
    analyze("url('x','y').description") should equal ("""text "y"""")
  }

  test("phone(x, y).phone_number == x") {
    analyze("phone('x','y').phone_number") should equal ("""text "x"""")
  }

  test("phone(x, y).phone_type == y") {
    analyze("phone('x','y').phone_type") should equal ("""text "y"""")
  }

  test("negative literals get their operator folded in") {
    analyze("-5") should equal ("""-5 :: numeric""")
  }

  test("negation operator is parenthesized") {
    analyze("-num") should equal ("""-(x1.num)""")
  }

  test("positive operator is just dropped") {
    analyze("+num") should equal ("""x1.num""")
  }

  test("geo literal") {
    // st_asbinary because this is the output expression for a soql string
    analyze("'POINT(10 10)'::point") should equal ("""st_asbinary(st_pointfromwkb(bytea "\\x000000000140240000000000004024000000000000", 4326))""")
  }

  test("date extract special-case") {
    analyze("date_extract_d('2001-01-01T12:34:56.789')") should equal ("""extract(day from (timestamp without time zone "2001-01-01T12:34:56.789"))""")
  }

  test("Functions are correctly classified") {
    // The "contains" check is because of the TsVector fake functions
    // that search gets rewritten into
    for(f <- sqlizer.funcallSqlizer.ordinaryFunctionMap.keysIterator.filter(SoQLFunctions.functionsByIdentity.contains)) {
      SoQLFunctions.functionsByIdentity(f).isAggregate should be (false)
      SoQLFunctions.functionsByIdentity(f).needsWindow should be (false)
    }

    for(f <- sqlizer.funcallSqlizer.aggregateFunctionMap.keysIterator) {
      SoQLFunctions.functionsByIdentity(f).isAggregate should be (true)
      SoQLFunctions.functionsByIdentity(f).needsWindow should be (false)
    }

    for(f <- sqlizer.funcallSqlizer.windowedFunctionMap.keysIterator) {
      val function = SoQLFunctions.functionsByIdentity(f)
      (function.isAggregate || function.needsWindow) should be (true)
    }
  }

  test("All functions are implemented") {
    for(f <- SoQLFunctions.allFunctions) {
      if(!(sqlizer.funcallSqlizer.ordinaryFunctionMap.contains(f.identity) || sqlizer.funcallSqlizer.aggregateFunctionMap.contains(f.identity) || sqlizer.funcallSqlizer.windowedFunctionMap.contains(f.identity))) {
        println("Not implemented: " + f.identity)
      }
    }
  }

  test("All aggregate functions are also window functions") {
    for(f <- SoQLFunctions.allFunctions if f.isAggregate) {
      if(!sqlizer.funcallSqlizer.windowedFunctionMap.contains(f.identity)) {
        println("Not implemented in windowed form: " + f.identity)
      }
    }
  }
}
