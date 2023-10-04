package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, MustMatchers}
import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.{ResourceName, Provenance}
import com.socrata.soql.functions._

object SoQLFunctionSqlizerTestPostgres {
  final abstract class TestMT extends MetaTypes {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Int
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  object ProvenanceMapper extends types.ProvenanceMapper[TestMT] {
    def toProvenance(dtn: types.DatabaseTableName[TestMT]): Provenance = {
      val DatabaseTableName(name) = dtn
      Provenance(name)
    }

    def fromProvenance(prov: Provenance): types.DatabaseTableName[TestMT] = {
      val Provenance(name) = prov
      DatabaseTableName(name)
    }
  }
}

class SoQLFunctionSqlizerTestPostgres extends FunSuite with MustMatchers with SqlizerUniverse[SoQLFunctionSqlizerTestPostgres.TestMT] {
  type TestMT = SoQLFunctionSqlizerTestPostgres.TestMT

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

    override val toProvenance = SoQLFunctionSqlizerTestPostgres.ProvenanceMapper
    def isRollup(dtn: DatabaseTableName) = false

    val cryptProvider = obfuscation.CryptProvider.zeros

    override def mkRepProvider(physicalTableFor: Map[AutoTableLabel, DatabaseTableName]) =
      new SoQLRepProviderPostgres[TestMT](_ => Some(cryptProvider), namespace, toProvenance, isRollup, Map.empty, physicalTableFor) {
        override def mkStringLiteral(s: String) = Doc(JString(s).toString)
      }

    override val funcallSqlizer = new SoQLFunctionSqlizerPostgres

    override val rewriteSearch = new SoQLRewriteSearch(searchBeforeQuery = true)

    override val systemContext = Map("hello" -> "world")
  }

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[TestMT](items.toMap)
  val analyzer = new SoQLAnalyzer[TestMT](SoQLTypeInfo, SoQLFunctionInfo, SoQLFunctionSqlizerTestPostgres.ProvenanceMapper)
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

    sqlizer(analysis).sql.layoutSingleLine.toString
  }

  test("basic search") {
    analyzeStatement("SELECT 1 SEARCH 'hello'") must equal("""SELECT 1 :: numeric AS i1 FROM table1 AS x1 WHERE (to_tsvector('english', ((((coalesce(x1.text, text "")) || (text " ")) || (coalesce(x1.url_url, text ""))) || (text " ")) || (coalesce(x1.url_description, text "")))) @@ (plainto_tsquery('english', text "hello"))""");
  }

  test("search with where") {
    analyzeStatement("SELECT 1 where num > 5 SEARCH 'hello'") must equal("""SELECT 1 :: numeric AS i1 FROM table1 AS x1 WHERE ((to_tsvector('english', ((((coalesce(x1.text, text "")) || (text " ")) || (coalesce(x1.url_url, text ""))) || (text " ")) || (coalesce(x1.url_description, text "")))) @@ (plainto_tsquery('english', text "hello"))) AND ((x1.num) > (5 :: numeric))""");
  }

  test("subquery search") {
    analyzeStatement("SELECT text, url |> select 1 SEARCH 'hello'") must equal("""SELECT 1 :: numeric AS i3 FROM (SELECT x1.text AS i1, x1.url_url AS i2_url, x1.url_description AS i2_description FROM table1 AS x1) AS x2 WHERE (to_tsvector('english', ((((coalesce(x2.i1, text "")) || (text " ")) || (coalesce(x2.i2_url, text ""))) || (text " ")) || (coalesce(x2.i2_description, text "")))) @@ (plainto_tsquery('english', text "hello"))""")
  }

  test("subquery search - compressed") {
    // the "case" here just forces url to be compressed
    analyzeStatement("SELECT text, case when true then url end |> select 1 SEARCH 'hello'") must equal ("""SELECT 1 :: numeric AS i3 FROM (SELECT x1.text AS i1, soql_compress_compound(x1.url_url, x1.url_description) AS i2 FROM table1 AS x1) AS x2 WHERE (to_tsvector('english', ((((coalesce(x2.i1, text "")) || (text " ")) || (coalesce((x2.i2) ->> 0, text ""))) || (text " ")) || (coalesce((x2.i2) ->> 1, text "")))) @@ (plainto_tsquery('english', text "hello"))""")
  }

  test("all window functions are covered") {
    sqlizer.funcallSqlizer.windowedFunctionMap.keySet == SoQLFunctions.windowFunctions.toSet
  }

  test("lead/lag gets its int cast") {
    analyze("lead(text, 5) OVER ()") must equal ("lead(x1.text, (5 :: numeric) :: int) OVER ()")
  }

  test("convex_hull gets its buffer wrapper") {
    analyze("convex_hull(geom)") must equal ("st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0)))")
  }

  test("concave_hull gets its buffer wrapper") {
    analyze("concave_hull(geom,3.0)") must equal ("st_asbinary(st_multi(st_buffer(st_concavehull(x1.geom, 3.0 :: numeric), 0.0)))")
  }

  test("things get correctly de-select-list-referenced") {
    // The convex_hull(geom) gets de-referenced because of introducing
    // the st_asbinary call, but the text||text continues to be
    // referenced.
    analyzeStatement("select convex_hull(geom) as a, text||text as b group by a, b", useSelectListReferences = true) must equal ("SELECT st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0))) AS i1, (x1.text) || (x1.text) AS i2 FROM table1 AS x1 GROUP BY st_multi(st_buffer(st_convexhull(x1.geom), 0.0)), 2")
  }

  test("case - else") {
    analyze("case when 1=1 then 'hello' when 1=2 then 'world' else 'otherwise' end") must equal ("""CASE WHEN (1 :: numeric) = (1 :: numeric) THEN text "hello" WHEN (1 :: numeric) = (2 :: numeric) THEN text "world" ELSE text "otherwise" END""")
  }

  test("case - no else") {
    analyze("case when 1=1 then 'hello' when 1=2 then 'world' end") must equal ("""CASE WHEN (1 :: numeric) = (1 :: numeric) THEN text "hello" WHEN (1 :: numeric) = (2 :: numeric) THEN text "world" END""")
  }

  test("iif") {
    analyze("iif(1 = 1, 2, 3)") must equal ("CASE WHEN (1 :: numeric) = (1 :: numeric) THEN 2 :: numeric ELSE 3 :: numeric END")
  }

  test("uncased works") {
    analyze("caseless_eq(text,'bleh')") must equal ("""(upper(x1.text)) = (upper(text "bleh"))""")
  }

  test("count gets cast to numeric") {
    analyze("count(*)") must equal ("""(count(*)) :: numeric""")
    analyze("count(text)") must equal ("""(count(x1.text)) :: numeric""")
    analyze("count(distinct text)") must equal ("""(count(DISTINCT x1.text)) :: numeric""")
  }

  test("get_context known literal") {
    analyze("get_context('hello')") must equal ("""text "world"""")
  }

  test("get_context unknown literal") {
    analyze("get_context('goodbye')") must equal ("""null :: text""")
  }

  test("get_context non-literal") {
    analyze("get_context(text)") must equal ("""current_setting('socrata_system.a' || md5(x1.text), true)""")
  }

  test("url(x, y).url == x") {
    analyze("url('x','y').url") must equal ("""text "x"""")
  }

  test("url(x, y).description == y") {
    analyze("url('x','y').description") must equal ("""text "y"""")
  }

  test("phone(x, y).phone_number == x") {
    analyze("phone('x','y').phone_number") must equal ("""text "x"""")
  }

  test("phone(x, y).phone_type == y") {
    analyze("phone('x','y').phone_type") must equal ("""text "y"""")
  }

  test("negative literals get their operator folded in") {
    analyze("-5") must equal ("""-5 :: numeric""")
  }

  test("negation operator is parenthesized") {
    analyze("-num") must equal ("""-(x1.num)""")
  }

  test("positive operator is just dropped") {
    analyze("+num") must equal ("""x1.num""")
  }

  test("geo literal") {
    // st_asbinary because this is the output expression for a soql string
    analyze("'POINT(10 10)'::point") must equal ("""st_asbinary(st_pointfromwkb(bytea "\\x000000000140240000000000004024000000000000", 4326))""")
  }

  test("date extract special-case") {
    analyze("date_extract_d('2001-01-01T12:34:56.789')") must equal ("""extract(day from (timestamp without time zone "2001-01-01T12:34:56.789"))""")
  }

  test("Functions are correctly classified") {
    // The "contains" check is because of the TsVector fake functions
    // that search gets rewritten into
    for(f <- sqlizer.funcallSqlizer.ordinaryFunctionMap.keysIterator.filter(SoQLFunctions.functionsByIdentity.contains)) {
      SoQLFunctions.functionsByIdentity(f).isAggregate must be (false)
      SoQLFunctions.functionsByIdentity(f).needsWindow must be (false)
    }

    for(f <- sqlizer.funcallSqlizer.aggregateFunctionMap.keysIterator) {
      SoQLFunctions.functionsByIdentity(f).isAggregate must be (true)
      SoQLFunctions.functionsByIdentity(f).needsWindow must be (false)
    }

    for(f <- sqlizer.funcallSqlizer.windowedFunctionMap.keysIterator) {
      val function = SoQLFunctions.functionsByIdentity(f)
      (function.isAggregate || function.needsWindow) must be (true)
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