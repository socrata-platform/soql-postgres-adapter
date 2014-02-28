package com.socrata.pg.soql

import org.scalatest.{Matchers, FunSuite}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

class SqlizerTest extends FunSuite with Matchers {

  import SqlizerTest._

  test("string literal with quotes") {
    val soql = "select 'there is a '' quote'"
    val ParametricSql(sql, setParams) = sqlize(soql)
    sql should be ("SELECT ? FROM t1")
    setParams.length should be (1)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("there is a ' quote" ))
  }

  test("field in (x, y...)") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    val ParametricSql(sql, setParams) = sqlize(soql)
    sql should be ("SELECT case_number FROM t1 WHERE case_number in(?,?,?) ORDER BY case_number nulls last LIMIT 2 OFFSET 1")
    setParams.length should be (3)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("ha001", "ha002", "ha003"))
  }

  test("expr and expr") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    val ParametricSql(sql, setParams) = sqlize(soql)
    sql should be ("SELECT id FROM t1 WHERE id = ? and case_number = ?")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, "cn001"))
  }

  test("starts_with has automatic suffix %") {
    val soql = "select id where starts_with(case_number, 'cn')"
    val ParametricSql(sql, setParams) = sqlize(soql)
    sql should be ("SELECT id FROM t1 WHERE case_number like ? || ?")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq("cn", "%"))
  }

  test("between") {
    val soql = "select id where id between 1 and 9"
    val ParametricSql(sql, setParams) = sqlize(soql)
    sql should be ("SELECT id FROM t1 WHERE id between ? and ?")
    setParams.length should be (2)
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    params should be (Seq(1, 9))
  }

}

object SqlizerTest {

  import Sqlizer._

  private val cryptProvider = new CryptProvider(CryptProvider.generateKey())
  private val idRep = new SoQLID.StringRep(cryptProvider)
  private val verRep = new SoQLVersion.StringRep(cryptProvider)

  private def sqlize(soql: String): ParametricSql = {
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    (analysis, "t1").sql(Map.empty[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], Seq.empty, idRep, verRep)
  }

  private val idMap =  (cn: ColumnName) => new UserColumnId(cn.caseFolded)

  private val columnMap = Map(
    ColumnName(":id") -> (1, SoQLID),
    ColumnName(":version") -> (2, SoQLVersion),
    ColumnName(":created_at") -> (3, SoQLFixedTimestamp),
    ColumnName(":updated_at") -> (4, SoQLFixedTimestamp),
    ColumnName("id") -> (5, SoQLNumber),
    ColumnName("case_number") -> (6, SoQLText),
    ColumnName("primary_type") -> (7, SoQLText),
    ColumnName("year") -> (8, SoQLNumber),
    ColumnName("arrest") -> (9, SoQLBoolean),
    ColumnName("updated_on") -> (10, SoQLFloatingTimestamp),
    ColumnName("location") -> (11, SoQLLocation),
    ColumnName("object") -> (12, SoQLObject),
    ColumnName("array") -> (13, SoQLArray)
  )

  private val datasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](columnMap,  columnMap.keys.toVector)
  }
}
