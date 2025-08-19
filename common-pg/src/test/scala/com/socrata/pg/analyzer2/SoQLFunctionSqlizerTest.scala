package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, MustMatchers}
import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.{ResourceName, Provenance}
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

object SoQLFunctionSqlizerTest {
  final abstract class TestMT extends MetaTypes with metatypes.SoQLMetaTypesExt {
    type ColumnType = SoQLType
    type ColumnValue = SoQLValue
    type ResourceNameScope = Int
    type DatabaseTableNameImpl = String
    type DatabaseColumnNameImpl = String
  }

  object TestNamespaces extends SqlNamespaces[TestMT] {
    override def rawDatabaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      name
    }

    override def rawDatabaseColumnName(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(name) = dcn
      name
    }

    override def rawDatabaseColumnName(dcn: DatabaseColumnName, suffix: String) = {
      val DatabaseColumnName(name) = dcn
      name + "_" + suffix
    }

    override def gensymPrefix: String = "g"
    protected override def idxPrefix: String ="idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
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

  val TestFuncallSqlizer = new SoQLFunctionSqlizer[TestMT]

  val TestExprSqlizer = new ExprSqlizer(
    TestFuncallSqlizer,
    new SoQLExprSqlFactory
  )

  val TestSqlizer = new Sqlizer[TestMT](
    TestExprSqlizer,
    TestNamespaces,
    new SoQLRewriteSearch[TestMT](searchBeforeQuery = true, SoQLRewriteSearch.simpleDcnComparator),
    ProvenanceMapper,
    _ => false,
    (sqlizer, physicalTableFor, extraContext) =>
      new SoQLRepProvider[TestMT](
        extraContext.cryptProviderProvider,
        sqlizer.exprSqlFactory,
        sqlizer.namespace,
        sqlizer.toProvenance,
        sqlizer.isRollup,
        Map.empty,
        physicalTableFor
      ) {
        override def mkStringLiteral(s: String) = Doc(extraContext.escapeString(s))
      }
  )
}

class SoQLFunctionSqlizerTest extends FunSuite with MustMatchers with SqlizerUniverse[SoQLFunctionSqlizerTest.TestMT] {
  type TestMT = SoQLFunctionSqlizerTest.TestMT

  val sqlizer = SoQLFunctionSqlizerTest.TestSqlizer
  val funcallSqlizer = SoQLFunctionSqlizerTest.TestFuncallSqlizer

  def extraContext = new SoQLExtraContext(
    Map("hello" -> "world"),
    new CryptProviderProvider {
      def forProvenance(provenance: Provenance) = Some(obfuscation.CryptProvider.zeros)
      def allProviders = Map.empty
    },
    Map.empty,
    JString(_).toString,
    new TimestampProvider.InProcess
  )

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[TestMT](items.toMap)
  val analyzer = new SoQLAnalyzer[TestMT](new SoQLTypeInfo2, SoQLFunctionInfo, SoQLFunctionSqlizerTest.ProvenanceMapper)
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
        "url" -> SoQLUrl,
        "text" -> SoQLText,
        "num" -> SoQLNumber,
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

    sqlizer(analysis, extraContext).getOrElse { fail("analysis failed") }.sql.layoutSingleLine.toString
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
    funcallSqlizer.windowedFunctionMap.keySet == SoQLFunctions.windowFunctions.toSet
  }

  test("Non-distinct percentile_cont gets annotated") {
    analyze("median(num)") must equal ("(percentile_cont(.50) within group (order by x1.num)) :: numeric")
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
    analyze("get_context(text)") must equal ("""pg_temp.dynamic_system_context(x1.text)""")
  }

  test("url(x, y).url == x") {
    analyze("url('x','y').url") must equal ("""text "x"""")
  }

  test("url(x, y).description == y") {
    analyze("url('x','y').description") must equal ("""text "y"""")
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
    for(f <- funcallSqlizer.ordinaryFunctionMap.keysIterator.filter(SoQLFunctions.functionsByIdentity.contains)) {
      SoQLFunctions.functionsByIdentity(f).isAggregate must be (false)
      SoQLFunctions.functionsByIdentity(f).needsWindow must be (false)
    }

    for(f <- funcallSqlizer.aggregateFunctionMap.keysIterator) {
      SoQLFunctions.functionsByIdentity(f).isAggregate must be (true)
      SoQLFunctions.functionsByIdentity(f).needsWindow must be (false)
    }

    for(f <- funcallSqlizer.windowedFunctionMap.keysIterator) {
      val function = SoQLFunctions.functionsByIdentity(f)
      (function.isAggregate || function.needsWindow) must be (true)
    }
  }

  test("All functions are implemented") {
    for(f <- SoQLFunctions.allFunctions) {
      if(!(funcallSqlizer.ordinaryFunctionMap.contains(f.identity) || funcallSqlizer.aggregateFunctionMap.contains(f.identity) || funcallSqlizer.windowedFunctionMap.contains(f.identity))) {
        println("Not implemented: " + f.identity)
      }
    }
  }

  test("All aggregate functions are also window functions") {
    for(f <- SoQLFunctions.allFunctions if f.isAggregate) {
      if(!funcallSqlizer.windowedFunctionMap.contains(f.identity)) {
        println("Not implemented in windowed form: " + f.identity)
      }
    }
  }
}
