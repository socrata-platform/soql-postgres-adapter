package com.socrata.soql.analyzer

import com.socrata.pg.soql._
import com.socrata.soql.{AnalysisDeserializer, AnalysisSerializer, SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.environment._
import com.socrata.soql.types.SoQLType
import java.io.{InputStream, OutputStream}

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.parsing.Parser

object SoQLAnalyzerHelper {
  private val serializer = new AnalysisSerializer(serializeColumn, serializeAnalysisType)

  private val deserializer = new AnalysisDeserializer(deserializeColumn,
                                                      deserializeType,
                                                      SoQLFunctions.functionsByIdentity)

  def serialize(outputStream: OutputStream, analyses: Seq[SoQLAnalysis[UserColumnId, SoQLType]]): Unit =
    serializer(outputStream, analyses)

  def deserialize(inputStream: InputStream): Seq[SoQLAnalysis[UserColumnId, SoQLType]] = deserializer(inputStream)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String,
                  datasetCtx: Map[String, DatasetContext[SoQLType]],
                  idMap: Map[QualifiedColumnName, UserColumnId]): Seq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    val parsed = new Parser().selectStatement(soql)
    val joins = parsed.flatten { select =>
      select.join match {
        case None => Seq.empty
        case Some(joins) => joins
      }
    }

    val joinColumnIdMap =
      joins.foldLeft(idMap) { (acc, join) =>
        val schema = datasetCtx(join.tableName.qualifier)
        acc ++ schema.columns.map { fieldName =>
          QualifiedColumnName(Some(join.tableName.qualifier), new ColumnName(fieldName.name)) ->
            new UserColumnId(fieldName.caseFolded)
        }
      }

    val analyses = analyzer.analyze(parsed)(datasetCtx)
    remapAnalyses(joinColumnIdMap, analyses)
  }

  /**
   * Remap chained analyses as each analysis may have different selection.
   * This function is for test setup and should mimic the the same function
   * in the "Query Coordinator" project QueryParser class.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                            analyses: Seq[SoQLAnalysis[ColumnName, SoQLType]])
    : Seq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    val initialAcc = (columnIdMapping, Seq.empty[SoQLAnalysis[UserColumnId, SoQLType]])
    val (_, analysesInColIds) = analyses.foldLeft(initialAcc) { (acc, analysis) =>
      val (mapping, convertedAnalyses) = acc
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.map(QualifiedColumnName(None, _)).filter { columnName => !mapping.contains(columnName) }
      val mappingWithNewColumns = newlyIntroducedColumns.foldLeft(mapping) { (acc, newColumn) =>
        acc + (newColumn -> new UserColumnId(newColumn.columnName.name))
      }
      // Re-map columns except for the innermost soql
      val newMapping =
        if (convertedAnalyses.nonEmpty) {
          val prevAnalysis = convertedAnalyses.last
          prevAnalysis.selection.foldLeft(mapping) { (acc, selCol) =>
            val (colName, expr) = selCol
            acc + (QualifiedColumnName(None, colName) -> new UserColumnId(colName.name))
          }
        } else {
          mappingWithNewColumns
        }

      val a: SoQLAnalysis[UserColumnId, SoQLType] = analysis.mapColumnIds(qualifiedColumnNameToColumnId(newMapping))
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    analysesInColIds
  }

  private def qualifiedColumnNameToColumnId[T](qualifiedColumnNameMap: Map[QualifiedColumnName, T])
                                              (columnName: ColumnName, qualifier: Option[String]): T = {
    qualifiedColumnNameMap(QualifiedColumnName(qualifier, columnName))
  }

  private def serializeColumn(c: UserColumnId) = c.underlying
  private def deserializeColumn(s: String) = new UserColumnId(s)

  private def serializeAnalysisType(t: SoQLType) = t.name.name
  private def deserializeType(s: String): SoQLType = SoQLType.typesByName(TypeName(s))
}
