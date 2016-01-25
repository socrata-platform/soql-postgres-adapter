package com.socrata.pg.soql

import org.scalatest.{Matchers, FunSuite}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.pg.store.index.SoQLIndexableRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider

// scalastyle:off null
class SqlizerTest extends FunSuite with Matchers

object SqlizerTest {
  private val cryptProvider = new CryptProvider(CryptProvider.generateKey())
  val sqlCtx = Map[SqlizerContext, Any](
    SqlizerContext.IdRep -> new SoQLID.StringRep(cryptProvider),
    SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider)
  )

  def sqlize(soql: String, caseSensitivity: CaseSensitivity): ParametricSql = {
    val allColumnReps = columnInfos.map(PostgresUniverseCommon.repForIndex(_))
    val analyses = SoQLAnalyzerHelper.analyzeSoQL(soql, datasetCtx, idMap)
    val typeReps: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]] =
      columnInfos.map { colInfo  =>
        (colInfo.typ -> SoQLRep.sqlRep(colInfo))
      }(collection.breakOut)

    SoQLAnalysisSqlizer.sql((analyses, "t1", allColumnReps))(
      columnReps,
      typeReps,
      Seq.empty,
      sqlCtx + (SqlizerContext.CaseSensitivity -> caseSensitivity),
      passThrough)
  }

  private val idMap =  (cn: ColumnName) => new UserColumnId(cn.caseFolded)

  private val passThrough = (s: String) => s

  private val columnMap = Map(
    ColumnName(":id") -> ((1, SoQLID)),
    ColumnName(":version") -> ((2, SoQLVersion)),
    ColumnName(":created_at") -> ((3, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((4, SoQLFixedTimestamp)),
    ColumnName("id") -> ((5, SoQLNumber)),
    ColumnName("case_number") -> ((6, SoQLText)),
    ColumnName("primary_type") -> ((7, SoQLText)),
    ColumnName("year") -> ((8, SoQLNumber)),
    ColumnName("arrest") -> ((9, SoQLBoolean)),
    ColumnName("updated_on") -> ((10, SoQLFloatingTimestamp)),
    ColumnName("object") -> ((11, SoQLObject)),
    ColumnName("array") -> ((12, SoQLArray)),
    ColumnName("point") -> ((13, SoQLPoint)),
    ColumnName("multiline") -> ((14, SoQLMultiLine)),
    ColumnName("multipolygon") -> ((15, SoQLMultiPolygon)),
    ColumnName("polygon") -> ((16, SoQLPolygon)),
    ColumnName("line") -> ((17, SoQLLine)),
    ColumnName("multipoint") -> ((18, SoQLMultiPoint)),
    ColumnName("location") -> ((19, SoQLLocation)),
    ColumnName("phone") -> ((20, SoQLPhone))
  )

  private val columnInfos = columnMap.foldLeft(Seq.empty[ColumnInfo[SoQLType]]) { (acc, colNameAndType) => colNameAndType match {
    case (columnName: ColumnName, (id, typ)) =>
      val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType](
        null,
        new ColumnId(id),
        new UserColumnId(columnName.caseFolded),
        typ,
        columnName.caseFolded,
        typ == SoQLID,
        false, // isUserKey
        typ == SoQLVersion
      )(SoQLTypeContext.typeNamespace, null)
      acc :+ cinfo
  }}

  private val datasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](columnMap,  columnMap.keys.toVector)
  }

  private val columnReps = {
    columnMap.map { case (colName, (_, typ)) =>
      idMap(colName) -> SoQLIndexableRep.sqlRep(typ, colName.name)
    }
  }
}
