package com.socrata.pg.analyzer2

import org.scalatest.{FunSuite, Matchers}
import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.types._
import com.socrata.soql.analyzer2._
import com.socrata.soql.analyzer2.mocktablefinder._
import com.socrata.soql.environment.ResourceName
import com.socrata.soql.sqlizer.SqlizerUniverse
import com.socrata.pg.store.PGSecondaryUniverseTestBase
import com.socrata.soql.functions._
import com.socrata.soql.sqlizer._

import com.typesafe.config.ConfigFactory
import com.socrata.pg.config.StoreConfig
import com.socrata.datacoordinator.common._
import com.socrata.datacoordinator.secondary.DatasetInfo
import com.socrata.soql.environment.Provenance

object SoQLFunctionSqlizerTestRedshift {
  final abstract class TestMT extends MetaTypes with metatypes.SoQLMetaTypesExt {
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

  object TestNamespaces extends SqlNamespaces[TestMT] {
    override def rawDatabaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      name
    }

    override def rawDatabaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(name) = dcn
      name
    }

    override def gensymPrefix: String = "g"
    protected override def idxPrefix: String ="idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
  }

  val TestFuncallSqlizer = new SoQLFunctionSqlizerRedshift[TestMT]

  val TestSqlizer = new Sqlizer[TestMT](
    TestFuncallSqlizer,
    new RedshiftExprSqlFactory,
    TestNamespaces,
    new SoQLRewriteSearch(searchBeforeQuery = true),
    ProvenanceMapper,
    _ => false,
    (sqlizer, physicalTableFor, extraContext) =>
      new SoQLRepProviderRedshift[TestMT](
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

class SoQLFunctionSqlizerTestRedshift extends FunSuite with Matchers with SqlizerUniverse[SoQLFunctionSqlizerTestRedshift.TestMT] with PGSecondaryUniverseTestBase {

  override val config = {
    val config = ConfigFactory.load()
    new StoreConfig(config.getConfig("com.socrata.pg.store.secondary"), "")
  }

  type TestMT = SoQLFunctionSqlizerTestRedshift.TestMT

  val sqlizer = SoQLFunctionSqlizerTestRedshift.TestSqlizer
  val funcallSqlizer = SoQLFunctionSqlizerTestRedshift.TestFuncallSqlizer

  def extraContext = new SoQLExtraContext(
    Map.empty,
    _ => Some(obfuscation.CryptProvider.zeros),
    Map.empty,
    s => s"'$s'"
  )

  // The easiest way to make an Expr for sqlization is just to analyze
  // it out...

  def tableFinder(items: ((Int, String), Thing[Int, SoQLType])*) =
    new MockTableFinder[TestMT](items.toMap)
  val analyzer = new SoQLAnalyzer[TestMT](SoQLTypeInfo, SoQLFunctionInfo, SoQLFunctionSqlizerTestRedshift.ProvenanceMapper)
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
        "geom" -> SoQLPolygon,
        "geometry_point" -> SoQLPoint
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

    val sql = sqlizer(analysis, extraContext).sql.layoutSingleLine.toString

    println(sql)
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
    analyzeStatement("SELECT left_pad(text, 10, 'a'), num") should equal("SELECT lpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1")
  }

  test("right_pad works") {
    analyzeStatement("SELECT right_pad(text, 10, 'a'), num") should equal("SELECT rpad(x1.text, (10 :: decimal(30, 7)) :: int, text 'a') AS i1, x1.num AS i2 FROM table1 AS x1")
  }

  test("chr() works") {
    analyzeStatement("SELECT chr(50.2)") should equal("SELECT chr((50.2 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
  }

  test("substring(characters, start_index base 1) works"){
    analyzeStatement("SELECT substring('abcdefghijk', 3)") should equal("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
  }

  test("substring(characters, start_index base 1, length) works") {
    analyzeStatement("SELECT substring('abcdefghijk', 3, 4)") should equal("SELECT substring(text 'abcdefghijk', (3 :: decimal(30, 7)) :: int, (4 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
  }

  test("split_part works") {
    analyzeStatement("SELECT split_part(text, '.', 3)") should equal("SELECT split_part(x1.text, text '.', (3 :: decimal(30, 7)) :: int) AS i1 FROM table1 AS x1")
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

  test("contains works") {
    analyzeStatement("SELECT text where contains(text, 'a')") should equal("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(text 'a' in x1.text) <> 0)""")
  }

  test("caseless contains works") {
    analyzeStatement("SELECT text where caseless_contains(text, 'o')") should equal("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* soql_contains */ position(upper(text 'o') in upper(x1.text)) <> 0)""")
  }

  test("starts_with works") {
    analyzeStatement("SELECT text where starts_with(text, 'o')") should equal("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ text 'o' = left(x1.text, length(text 'o')))""")
  }

  test("caseless starts_with works") {
    analyzeStatement("SELECT text where caseless_starts_with(text, 'o')") should equal("""SELECT x1.text AS i1 FROM table1 AS x1 WHERE (/* start_with */ upper(text 'o') = left(upper(x1.text), length(upper(text 'o'))))""")
  }

  test("round works") {
    analyzeStatement("SELECT text, round(num, 2)") should equal("""SELECT x1.text AS i1, (/* soql_round */ round(x1.num, 2 :: decimal(30, 7) :: int) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("ToFloatingTimestamp") {
    analyze("""to_floating_timestamp("2022-12-31T23:59:59Z", "America/New_York")""") should equal(
      """(timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York')"""
    )
  }

  test("FloatingTimeStampTruncYmd") {
    analyze("date_trunc_ymd('2022-12-31T23:59:59')") should equal(
      """date_trunc('day', timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampTruncYm") {
    analyze("date_trunc_ym('2022-12-31T23:59:59')") should equal(
      """date_trunc('month', timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampTruncY") {
    analyze("date_trunc_y('2022-12-31T23:59:59')") should equal(
      """date_trunc('year', timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FixedTimeStampZTruncYmd") {
    analyze("datez_trunc_ymd('2022-12-31T23:59:59Z')") should equal(
      """date_trunc('day', timestamp with time zone '2022-12-31T23:59:59.000Z')"""
    )
  }

  test("FixedTimeStampZTruncYm") {
    analyze("datez_trunc_ym('2022-12-31T23:59:59Z')") should equal(
      """date_trunc('month', timestamp with time zone '2022-12-31T23:59:59.000Z')"""
    )
  }

  test("FixedTimeStampZTruncY") {
    analyze("datez_trunc_y('2022-12-31T23:59:59Z')") should equal(
      """date_trunc('year', timestamp with time zone '2022-12-31T23:59:59.000Z')"""
    )
  }

  test("FixedTimeStampTruncYmdAtTimeZone") {
    analyze("date_trunc_ymd('2022-12-31T23:59:59Z', 'America/New_York')") should equal(
      """date_trunc('day', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""
    )
  }

  test("FixedTimeStampTruncYmAtTimeZone") {
    analyze("date_trunc_ym('2022-12-31T23:59:59Z', 'America/New_York')") should equal(
      """date_trunc('month', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""
    )
  }

  test("FixedTimeStampTruncYAtTimeZone") {
    analyze("date_trunc_y('2022-12-31T23:59:59Z', 'America/New_York')") should equal(
      """date_trunc('year', (timestamp with time zone '2022-12-31T23:59:59.000Z') at time zone (text 'America/New_York'))"""
    )
  }

  test("FloatingTimeStampExtractY") {
    analyze("date_extract_y('2022-12-31T23:59:59')") should equal(
      """extract(year from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractM") {
    analyze("date_extract_m('2022-12-31T23:59:59')") should equal(
      """extract(month from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractD") {
    analyze("date_extract_d('2022-12-31T23:59:59')") should equal(
      """extract(day from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractHh") {
    analyze("date_extract_hh('2022-12-31T23:59:59')") should equal(
      """extract(hour from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractMm") {
    analyze("date_extract_mm('2022-12-31T23:59:59')") should equal(
      """extract(minute from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractSs") {
    analyze("date_extract_ss('2022-12-31T23:59:59')") should equal(
      """extract(second from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractDow") {
    analyze("date_extract_dow('2022-12-31T23:59:59')") should equal(
      """extract(dayofweek from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimeStampExtractWoy") {
    analyze("date_extract_woy('2022-12-31T23:59:59')") should equal(
      """extract(week from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("FloatingTimestampExtractIsoY") {
    analyze("date_extract_iso_y('2022-12-31T23:59:59')") should equal(
      """extract(year from timestamp without time zone '2022-12-31T23:59:59.000')"""
    )
  }

  test("EpochSeconds") {
    analyze("epoch_seconds('2022-12-31T23:59:59Z')") should equal(
      """extract(epoch from timestamp with time zone '2022-12-31T23:59:59.000Z')"""
    )
  }

  test("TimeStampDiffD") {
    analyze("date_diff_d('2022-12-31T23:59:59Z', '2022-01-01T00:00:00Z')") should equal(
      """datediff(day, timestamp with time zone '2022-12-31T23:59:59.000Z' at time zone (text 'UTC'), timestamp with time zone '2022-01-01T00:00:00.000Z' at time zone (text 'UTC'))"""
    )
  }

  test("SignedMagnitude10") {
    analyzeStatement("select signed_magnitude_10(num)") should equal("""SELECT ((/* soql_signed_magnitude_10 */ (sign(x1.num) * length(floor(abs(x1.num)) :: text)))) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("SignedMagnitudeLinear") {
    analyzeStatement("select signed_magnitude_Linear(num, 8)") should equal("""SELECT ((/* soql_signed_magnitude_linear */ (case when (8 :: decimal(30, 7)) = 1 then floor(x1.num) else sign(x1.num) * floor(abs(x1.num)/(8 :: decimal(30, 7)) + 1) end))) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("TimeStampAdd") {
    analyze("date_add('2022-12-31T23:59:59Z', 'P1DT1H')") should equal(
      """(timestamp with time zone '2022-12-31T23:59:59.000Z') + (interval '1 days, 1 hours')"""
    )
  }

  test("TimeStampPlus") {
    analyze("('2022-12-31T23:59:59Z' + 'P1001Y1DT1H1S')") should equal(
      """(timestamp with time zone '2022-12-31T23:59:59.000Z') + (interval '1 millenniums, 1 years, 1 days, 1 hours, 1 seconds')"""
    )
  }

  test("TimeStampMinus") {
    analyze("('2022-12-31T23:59:59Z' - 'P1001Y1DT1H1S')") should equal(
      """(timestamp with time zone '2022-12-31T23:59:59.000Z') - (interval '1 millenniums, 1 years, 1 days, 1 hours, 1 seconds')"""
    )
  }

  test("GetUtcDate") {
    analyze("get_utc_date()") should equal(
      """current_date at time zone 'UTC'"""
    )
  }

//  tests for aggregate functions
  test("max works") {
    analyzeStatement("SELECT max(num)") should equal("""SELECT max(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("min works") {
    analyzeStatement("SELECT min(num)") should equal("""SELECT min(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("count(*) works") {
    analyzeStatement("SELECT count(*)") should equal("""SELECT (count(*)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("count() works") {
    analyzeStatement("SELECT count(text)") should equal("""SELECT (count(x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("count(iif ... ) works") {
    analyzeStatement("SELECT count(IIF(text='one', 1, NULL))") should equal("""SELECT (count(CASE WHEN (x1.text) = (text 'one') THEN 1 :: decimal(30, 7) ELSE null :: decimal(30, 7) END)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("count_distinct works") {
    analyzeStatement("SELECT count_distinct(text)") should equal("""SELECT (count(DISTINCT x1.text)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("sum works") {
    analyzeStatement("SELECT sum(num)") should equal("""SELECT sum(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("avg works") {
    analyzeStatement("SELECT avg(num)") should equal("""SELECT avg(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("median works") {
    analyzeStatement("SELECT median(num)") should equal("""SELECT median(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("stddev_pop works") {
    analyzeStatement("SELECT stddev_pop(num)") should equal("""SELECT stddev_pop(x1.num) AS i1 FROM table1 AS x1""")
  }

  test("stddev_samp") {
    analyzeStatement("SELECT stddev_samp(num)") should equal("""SELECT stddev_samp(x1.num) AS i1 FROM table1 AS x1""")
  }

//  tests for conditional functions
  test("nullif works") {
    analyzeStatement("SELECT text, nullif(num, 1)") should equal("""SELECT x1.text AS i1, nullif(x1.num, 1 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("coalesce works") {
    analyzeStatement("SELECT coalesce(text, 'zero'), coalesce(num, '0')") should equal("""SELECT coalesce(x1.text, text 'zero') AS i1, coalesce(x1.num, 0 :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("case works") {
    analyzeStatement("SELECT num, case(num > 1, 'large num', num <= 1, 'small num')") should equal("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' WHEN (x1.num) <= (1 :: decimal(30, 7)) THEN text 'small num' END AS i2 FROM table1 AS x1""")
  }

  test("iif works") {
    analyzeStatement("SELECT num, iif(num > 1, 'large num', 'small num')") should equal("""SELECT x1.num AS i1, CASE WHEN (x1.num) > (1 :: decimal(30, 7)) THEN text 'large num' ELSE text 'small num' END AS i2 FROM table1 AS x1""")
  }

//  tests for geo functions
  test("spacial union works for polygons and points") {
    analyzeStatement("Select spatial_union(geom, geom)") should equal("""SELECT st_asbinary(st_multi(st_union(x1.geom, x1.geom))) AS i1 FROM table1 AS x1""")
  }

  test("from line to multiline, from polygon to multipolygon and from line to multiline work") {
    analyzeStatement("SELECT geo_multi(geom)") should equal("""SELECT st_asbinary(st_multi(x1.geom)) AS i1 FROM table1 AS x1""")
  }

  test("num_points works") {
    analyzeStatement("SELECT num_points(geom)") should equal("""SELECT (st_npoints(x1.geom)) :: decimal(30, 7) AS i1 FROM table1 AS x1""")
  }

  test("crosses works") {
    analyzeStatement("SELECT crosses(geom, geom)") should equal("""SELECT st_crosses(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
  }

  test("intersection works") {
    analyzeStatement("SELECT polygon_intersection(geom, geom)") should equal("""SELECT st_asbinary(st_multi(st_buffer(st_intersection(x1.geom, x1.geom), 0.0))) AS i1 FROM table1 AS x1""")
  }

  test("intersects works") {
    analyzeStatement("SELECT intersects(geom, geom)") should equal("""SELECT st_intersects(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
  }

  test("within_polygon works") {
    analyzeStatement("SELECT within_polygon(geom, geom)") should equal("""SELECT st_within(x1.geom, x1.geom) AS i1 FROM table1 AS x1""")
  }

  test("within_box works") {
    analyzeStatement("SELECT text, within_box(geom, 23, 34, 10, 56)") should equal("""SELECT x1.text AS i1, (/* within_box */ st_contains(st_makeenvelope(34 :: decimal(30, 7) :: DOUBLE PRECISION, 10 :: decimal(30, 7) :: DOUBLE PRECISION, 56 :: decimal(30, 7) :: DOUBLE PRECISION, 23 :: decimal(30, 7) :: DOUBLE PRECISION), x1.geom)) AS i2 FROM table1 AS x1""")
  }

  test("is_empty works") {
    analyzeStatement("SELECT text, is_empty(geom)") should equal("""SELECT x1.text AS i1, st_isempty(x1.geom) or (x1.geom) is null AS i2 FROM table1 AS x1""")
  }

  test("simplify works") {
    analyzeStatement("SELECT text, simplify(geom, 1)") should equal("""SELECT x1.text AS i1, st_asbinary(st_simplify(x1.geom, 1 :: decimal(30, 7))) AS i2 FROM table1 AS x1""")
  }

  test("area works") {
    analyzeStatement("SELECT text, area(geom)") should equal("""SELECT x1.text AS i1, (/* soql_area */ st_area(x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("distance_in_meters works for points only") {
    analyzeStatement("SELECT text, distance_in_meters(geom, geom)") should equal("""SELECT x1.text AS i1, (/* soql_distance_in_meters */ st_distance(x1.geom :: geography, x1.geom :: geography) :: decimal(30, 7)) AS i2 FROM table1 AS x1""")
  }

  test("visible_at works") {
    analyzeStatement("SELECT text, visible_at(geom, 3)") should equal("""SELECT x1.text AS i1, (/* soql_visible_at */ (not st_isempty(x1.geom)) AND (st_geometrytype(x1.geom) = 'ST_Point' OR st_geometrytype(x1.geom) = 'ST_MultiPoint' OR (ST_XMax(x1.geom) - ST_XMin(x1.geom)) >= 3 :: decimal(30, 7) OR (ST_YMax(x1.geom) - ST_YMin(x1.geom)) >= 3 :: decimal(30, 7))) AS i2 FROM table1 AS x1""")
  }

  test("convex_hull works") {
    analyzeStatement("SELECT text, convex_hull(geom)") should equal("""SELECT x1.text AS i1, st_asbinary(st_multi(st_buffer(st_convexhull(x1.geom), 0.0))) AS i2 FROM table1 AS x1""")
  }

  test("curated_region_test works") {
    analyzeStatement("Select text, curated_region_test(geom, 5)") should equal("""SELECT x1.text AS i1, (/* soql_curated_region_test */ case when st_npoints(x1.geom) > 5 :: decimal(30, 7) then 'too complex' when st_xmin(x1.geom) < -180 or st_xmax(x1.geom) > 180 or st_ymin(x1.geom) < -90 or st_ymax(x1.geom) > 90 then 'out of bounds' when not st_isvalid(x1.geom) then 'invalid geography data' when x1.geom is null then 'empty' end) AS i2 FROM table1 AS x1""")
  }

  test("test for PointToLatitude") {
    analyzeStatement("SELECT text, point_latitude(geometry_point)") should equal("""SELECT x1.text AS i1, (st_y(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1""")
  }

  test("test for PointToLongitude") {
    analyzeStatement("SELECT text, point_longitude(geometry_point)") should equal("""SELECT x1.text AS i1, (st_x(x1.geometry_point)) :: decimal(30, 7) AS i2 FROM table1 AS x1""")
  }

//  tests for window functions
  test("row_number works") {
    analyzeStatement("SELECT text, row_number() over(partition by text)") should equal("""SELECT x1.text AS i1, row_number() OVER (PARTITION BY x1.text) AS i2 FROM table1 AS x1""")
  }

  test("rank works") {
    analyzeStatement("SELECT text, rank() over(order by text)") should equal("""SELECT x1.text AS i1, rank() OVER (ORDER BY x1.text ASC NULLS LAST) AS i2 FROM table1 AS x1""")
  }

  test("dense_rank works") {
    analyzeStatement("SELECT text, num, dense_rank() over(partition by text order by num)") should equal("""SELECT x1.text AS i1, x1.num AS i2, dense_rank() OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST) AS i3 FROM table1 AS x1""")
  }

  test("first_value works") {
    analyzeStatement("SELECT text, num, first_value(num) over(partition by text order by num rows between unbounded preceding and current row)") should equal("""SELECT x1.text AS i1, x1.num AS i2, first_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1""")
  }

  test("last_value works") {
    analyzeStatement("SELECT text, num, last_value(num) over(partition by text order by num rows between unbounded preceding and current row)") should equal("""SELECT x1.text AS i1, x1.num AS i2, last_value(x1.num) OVER (PARTITION BY x1.text ORDER BY x1.num ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS i3 FROM table1 AS x1""")
  }
}


