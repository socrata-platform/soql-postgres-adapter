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
    override def databaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(name) = dtn
      Doc(name)
    }

    override def databaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(name) = dcn
      Doc(name)
    }

    protected override def gensymPrefix: String = "g"
    protected override def idxPrefix: String ="idx"
    protected override def autoTablePrefix: String = "x"
    protected override def autoColumnPrefix: String = "i"
  }
}

class SoQLFunctionSqlizerTestRedshift extends FunSuite with Matchers with SqlizerUniverse[SoQLFunctionSqlizerTestRedshift.TestMT] with PGSecondaryUniverseTestBase {

  override val config = {
    val config = ConfigFactory.load()
    new StoreConfig(config.getConfig("com.socrata.pg.store.secondary"), "")
  }

  type TestMT = SoQLFunctionSqlizerTestRedshift.TestMT

  val sqlizer = new Sqlizer {
    override val exprSqlFactory = new RedshiftExprSqlFactory[TestMT]
    override val namespace = SoQLFunctionSqlizerTestRedshift.TestNamespaces

    override val toProvenance = SoQLFunctionSqlizerTestRedshift.ProvenanceMapper
    def isRollup(dtn: DatabaseTableName) = false

    val cryptProvider = obfuscation.CryptProvider.zeros

    override def mkRepProvider(physicalTableFor: Map[AutoTableLabel, DatabaseTableName]) =
      new SoQLRepProviderRedshift[TestMT](_ => Some(cryptProvider), new RedshiftExprSqlFactory[TestMT], namespace, toProvenance, isRollup, Map.empty, physicalTableFor) {
        override def mkStringLiteral(s: String) = Doc(s"'$s'")
      }


    override val funcallSqlizer = new SoQLFunctionSqlizerRedshift[TestMT]

    override val rewriteSearch = new SoQLRewriteSearch[TestMT](searchBeforeQuery = true)

    override val systemContext = Map("hello" -> "world")
  }

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

    val sql = sqlizer(analysis, new SoQLExtraContext).sql.layoutSingleLine.toString

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
}
