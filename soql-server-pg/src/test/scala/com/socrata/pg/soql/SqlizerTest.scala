package com.socrata.pg.soql

import org.scalatest.{Matchers, FunSuite}
import com.socrata.soql.environment.{DatasetContext, ColumnName}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.types._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.SoQLAnalysis

class SqlizerTest extends FunSuite with Matchers {

  import SqlizerTest._

  test("string literal with quotes") {
    val soql = "select 'there is a '' quote'"
    sql(soql) should be ("SELECT 'there is a '' quote'")
  }

  test("field in (x, y...)") {
    val soql = "select case_number where case_number in ('ha001', 'ha002', 'ha003') order by case_number offset 1 limit 2"
    sql(soql) should be ("SELECT case_number WHERE case_number in('ha001','ha002','ha003') ORDER BY case_number nulls last LIMIT 2 OFFSET 1")
  }

  test("expr and expr") {
    val soql = "select id where id = 1 and case_number = 'cn001'"
    sql(soql) should be ("SELECT id WHERE id = 1 and case_number = 'cn001'")
  }
}

object SqlizerTest {

  import Sqlizer._

  private def sql(soql: String): String = {
    val analysis: SoQLAnalysis[UserColumnId, SoQLType] = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    analysis.sql
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
