package com.socrata.pg.soql

import org.scalatest.{FunSuite, Matchers}
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.common.soql.{SoQLRep, SoQLTypeContext}
import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.pg.store.index.SoQLIndexableRep
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.analyzer.{QualifiedColumnName, SoQLAnalyzerHelper}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName}
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

  val typeTable = TableName("_type", None)
  val yearTable = TableName("_year", Some("_y"))
  val catTable = TableName("_cat", None)
  val dogTable = TableName("_dog", None)
  val birdTable = TableName("_bird", None)
  val fishTable = TableName("_fish", None)

  def sqlize(soql: String, caseSensitivity: CaseSensitivity, useRepsWithId: Boolean = false, leadingSearch: Boolean = false): ParametricSql = {
    val allColumnReps = columnInfos.map(PostgresUniverseCommon.repForIndex(_))
    val allDatasetCtx = Map(TableName.PrimaryTable.qualifier -> datasetCtx,
                            typeTable.qualifier -> typeDatasetCtx,
                            yearTable.name -> yearDatasetCtx,
                            yearTable.qualifier -> yearDatasetCtx,
                            catTable.qualifier -> catDatasetCtx,
                            dogTable.qualifier -> dogDatasetCtx,
                            birdTable.qualifier -> birdDatasetCtx,
                            fishTable.qualifier -> fishDatasetCtx)

    val qualifiedUserIdColumnInfoMap: Map[QualifiedUserColumnId, ColumnInfo[SoQLType]] = qualifiedColumnIdToColumnInfos(TableName.PrimaryTable, columnMap) ++
      qualifiedColumnIdToColumnInfos(typeTable, typeTableColumnMap) ++
      qualifiedColumnIdToColumnInfos(yearTable, yearTableColumnMap) ++
      qualifiedColumnIdToColumnInfos(catTable, catTableColumnMap) ++
      qualifiedColumnIdToColumnInfos(dogTable, dogTableColumnMap) ++
      qualifiedColumnIdToColumnInfos(birdTable, birdTableColumnMap) ++
      qualifiedColumnIdToColumnInfos(fishTable, fishTableColumnMap)

    val columnRepsWithId = qualifiedUserIdColumnInfoMap.mapValues { ci =>
      PostgresUniverseCommon.repForIndex(ci)
    }

    val analyses = SoQLAnalyzerHelper.analyzeSoQL(soql, allDatasetCtx, allColumnIdMap)
    val typeReps: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]] =
      columnInfos.map { colInfo  =>
        (colInfo.typ -> SoQLRep.sqlRep(colInfo))
      }(collection.breakOut)


    val tableMap = Map(TableName.PrimaryTable -> "t1", typeTable -> "t2", yearTable -> "t3", yearTable.copy(alias = None) -> "t3",
      catTable -> "t11", dogTable -> "t12", birdTable -> "t13", fishTable -> "t14")
    BinarySoQLAnalysisSqlizer.sql((analyses, tableMap, allColumnReps))(
      if (useRepsWithId) columnRepsWithId else columnReps,
      typeReps,
      Seq.empty,
      sqlCtx + (SqlizerContext.CaseSensitivity -> caseSensitivity) + (SqlizerContext.LeadingSearch -> leadingSearch),
      passThrough)
  }

  private val idMap = (cn: ColumnName) => new UserColumnId(cn.caseFolded)

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
    ColumnName("phone") -> ((20, SoQLPhone)),
    ColumnName("url") -> ((21, SoQLUrl)),
    ColumnName("document") -> ((22, SoQLDocument)),
    ColumnName("json") -> ((23, SoQLJson))
  )

  private val typeTableColumnMap = Map(
    ColumnName(":id") -> ((21, SoQLID)),
    ColumnName(":version") -> ((22, SoQLVersion)),
    ColumnName(":created_at") -> ((23, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((24, SoQLFixedTimestamp)),
    ColumnName("primary_type") -> ((25, SoQLText)),
    ColumnName("description") -> ((26, SoQLText)),
    ColumnName("registered") -> ((26, SoQLFloatingTimestamp))
  )

  private val yearTableColumnMap = Map(
    ColumnName(":id") -> ((31, SoQLID)),
    ColumnName(":version") -> ((32, SoQLVersion)),
    ColumnName(":created_at") -> ((33, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((34, SoQLFixedTimestamp)),
    ColumnName("year") -> ((35, SoQLNumber)),
    ColumnName("avg_temperature") -> ((36, SoQLNumber))
  )

  private val catTableColumnMap = Map(
    ColumnName(":id") -> ((41, SoQLID)),
    ColumnName(":version") -> ((42, SoQLVersion)),
    ColumnName(":created_at") -> ((43, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((44, SoQLFixedTimestamp)),
    ColumnName("name") -> ((45, SoQLText)),
    ColumnName("breed") -> ((46, SoQLText)),
    ColumnName("age") -> ((47, SoQLNumber)),
    ColumnName("cat") -> ((48, SoQLText))
  )

  private val dogTableColumnMap = Map(
    ColumnName(":id") -> ((51, SoQLID)),
    ColumnName(":version") -> ((52, SoQLVersion)),
    ColumnName(":created_at") -> ((53, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((54, SoQLFixedTimestamp)),
    ColumnName("name") -> ((55, SoQLText)),
    ColumnName("breed") -> ((56, SoQLText)),
    ColumnName("age") -> ((57, SoQLNumber)),
    ColumnName("dog") -> ((58, SoQLText))
  )

  private val birdTableColumnMap = Map(
    ColumnName(":id") -> ((61, SoQLID)),
    ColumnName(":version") -> ((62, SoQLVersion)),
    ColumnName(":created_at") -> ((63, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((64, SoQLFixedTimestamp)),
    ColumnName("name") -> ((65, SoQLText)),
    ColumnName("breed") -> ((66, SoQLText)),
    ColumnName("age") -> ((67, SoQLNumber)),
    ColumnName("bird") -> ((68, SoQLText))
  )

  private val fishTableColumnMap = Map(
    ColumnName(":id") -> ((61, SoQLID)),
    ColumnName(":version") -> ((62, SoQLVersion)),
    ColumnName(":created_at") -> ((63, SoQLFixedTimestamp)),
    ColumnName(":updated_at") -> ((64, SoQLFixedTimestamp)),
    ColumnName("name") -> ((65, SoQLText)),
    ColumnName("breed") -> ((66, SoQLText)),
    ColumnName("age") -> ((67, SoQLNumber)),
    ColumnName("fish") -> ((68, SoQLText))
  )

  private val allColumnMap = columnMap ++ typeTableColumnMap ++ yearTableColumnMap ++
    catTableColumnMap ++ dogTableColumnMap ++ birdTableColumnMap ++ fishTableColumnMap

  private val allColumnIdMap =
    columnMap.map { case (k, (id, typ)) =>
      if (k.name == ":id") {
        QualifiedColumnName(None, k) -> new UserColumnId(s"${k.caseFolded}_$id")
      } else {
        QualifiedColumnName(None, k) -> new UserColumnId(s"${k.caseFolded}")
      }
    } ++
    typeTableColumnMap.map { case (k, v) =>
      QualifiedColumnName(Some(typeTable.qualifier), k) -> new UserColumnId(k.caseFolded)
    } ++
    yearTableColumnMap.map { case (k, v) =>
      QualifiedColumnName(Some(yearTable.name), k) -> new UserColumnId(k.caseFolded)
    } ++
    yearTableColumnMap.map { case (k, v) =>
      QualifiedColumnName(Some(yearTable.qualifier), k) -> new UserColumnId(k.caseFolded)
    } ++
    catTableColumnMap.map { case (k, (id, typ)) =>
      QualifiedColumnName(Some(catTable.qualifier), k) -> new UserColumnId(s"${k.caseFolded}_$id")
    } ++
    dogTableColumnMap.map { case (k, (id, typ)) =>
      QualifiedColumnName(Some(dogTable.qualifier), k) -> new UserColumnId(s"${k.caseFolded}_$id")
    } ++
    birdTableColumnMap.map { case (k, (id, typ)) =>
      QualifiedColumnName(Some(birdTable.qualifier), k) -> new UserColumnId(s"${k.caseFolded}_$id")
    } ++
    fishTableColumnMap.map { case (k, (id, typ)) =>
      QualifiedColumnName(Some(fishTable.qualifier), k) -> new UserColumnId(s"${k.caseFolded}_$id")
    }

  private val columnInfos = allColumnMap.foldLeft(Seq.empty[ColumnInfo[SoQLType]]) { (acc, colNameAndType) =>
    colNameAndType match {
      case (columnName: ColumnName, (id, typ)) =>
        val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType](
          null,
          new ColumnId(id),
          new UserColumnId(columnName.caseFolded),
          None,
          typ,
          columnName.caseFolded,
          typ == SoQLID,
          false, // isUserKey
          typ == SoQLVersion,
          None,
          Seq.empty
        )(SoQLTypeContext.typeNamespace, null)
        acc :+ cinfo
    }
  }

  private def qualifiedColumnIdToColumnInfos(tableName: TableName, columns: Map[ColumnName, (Int, SoQLType)]) =
    columns.foldLeft(Map.empty[QualifiedUserColumnId, ColumnInfo[SoQLType]]) { (acc, colNameAndType) =>
      colNameAndType match {
        case (columnName: ColumnName, (id, typ)) =>
          val cinfo = new com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType](
            null,
            new ColumnId(id),
            new UserColumnId(columnName.caseFolded),
            None,
            typ,
            columnName.caseFolded,
            typ == SoQLID,
            false, // isUserKey
            typ == SoQLVersion,
            None,
            Seq.empty
          )(SoQLTypeContext.typeNamespace, null)
          val qualifier = if (tableName == TableName.PrimaryTable) None else Some(tableName.qualifier)
          acc + (QualifiedUserColumnId(qualifier, idMap(columnName)) -> cinfo)
      }
  }

  private val datasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](columnMap,  columnMap.keys.toVector)
  }

  private val typeDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](typeTableColumnMap,  yearTableColumnMap.keys.toVector)
  }

  private val yearDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](yearTableColumnMap,  yearTableColumnMap.keys.toVector)
  }

  private val catDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](catTableColumnMap, catTableColumnMap.keys.toVector)
  }

  private val dogDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](dogTableColumnMap, dogTableColumnMap.keys.toVector)
  }

  private val birdDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](birdTableColumnMap, birdTableColumnMap.keys.toVector)
  }

  private val fishDatasetCtx: DatasetContext[SoQLType] = new DatasetContext[SoQLType] {
    val schema = new OrderedMap[ColumnName, SoQLType](fishTableColumnMap, fishTableColumnMap.keys.toVector)
  }

  private val columnReps = {
    columnMap.map { case (colName, (_, typ)) =>
      QualifiedUserColumnId(None, idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
    } ++
      typeTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(typeTable.qualifier), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      yearTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(yearTable.name), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      yearTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(yearTable.qualifier), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      catTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(catTable.name), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      dogTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(dogTable.name), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      birdTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(birdTable.name), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      } ++
      fishTableColumnMap.map { case (colName, (_, typ)) =>
        QualifiedUserColumnId(Some(fishTable.name), idMap(colName)) -> SoQLIndexableRep.sqlRep(typ, colName.name)
      }
  }
}
