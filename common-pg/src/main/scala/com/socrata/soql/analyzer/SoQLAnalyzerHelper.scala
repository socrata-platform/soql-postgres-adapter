package com.socrata.soql.analyzer

import com.socrata.pg.soql._
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer, AnalysisDeserializer, AnalysisSerializer}
import com.socrata.soql.functions.{SoQLFunctions, SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.environment.{ColumnName, TypeName, DatasetContext}
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType}
import java.io.{OutputStream, InputStream, ByteArrayInputStream, ByteArrayOutputStream}
import com.socrata.datacoordinator.id.UserColumnId

object SoQLAnalyzerHelper {
  private val serializer = new AnalysisSerializer(serializeColumn, serializeAnalysisType)

  private val deserializer = new AnalysisDeserializer(deserializeColumn,
                                                      deserializeType,
                                                      SoQLFunctions.functionsByIdentity)

  def serialize(outputStream: OutputStream, analyses: Seq[SoQLAnalysis[UserColumnId, SoQLAnalysisType]]): Unit =
    serializer(outputStream, analyses)

  def deserialize(inputStream: InputStream): Seq[SoQLAnalysis[UserColumnId, SoQLType]] = deserializer(inputStream)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String,
                  datasetCtx: DatasetContext[SoQLType],
                  idMap: ColumnName => UserColumnId): Seq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    implicit val ctx: DatasetContext[SoQLAnalysisType] = toAnalysisType(datasetCtx)

    val analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]] = analyzer.analyzeFullQuery(soql)
    val baos = new ByteArrayOutputStream
    serialize(baos, analyses.map(_.mapColumnIds(idMap)))
    deserialize(new ByteArrayInputStream(baos.toByteArray))
  }

  def analyzeSoQL(soql: String,
                  datasetCtx: DatasetContext[SoQLType],
                  columnIdMapping: Map[ColumnName, UserColumnId]): Seq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    implicit val ctx: DatasetContext[SoQLAnalysisType] = toAnalysisType(datasetCtx)

    val analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]] = analyzer.analyzeFullQuery(soql)
    val remappedAnalyses = remapAnalyses(columnIdMapping, analyses)

    val baos = new ByteArrayOutputStream
    serialize(baos, remappedAnalyses)
    deserialize(new ByteArrayInputStream(baos.toByteArray))
  }

  /**
   * Remap chained analyses as each analysis may have different selection.
   * This function is for test setup and should mimic the the same function
   * in the "Query Coordinator" project QueryParser class.
   */
  private def remapAnalyses(columnIdMapping: Map[ColumnName, UserColumnId],
                            analyses: Seq[SoQLAnalysis[ColumnName, SoQLAnalysisType]])
    : Seq[SoQLAnalysis[UserColumnId, SoQLAnalysisType]] = {
    val initialAcc = (columnIdMapping, Seq.empty[SoQLAnalysis[UserColumnId, SoQLAnalysisType]])
    val (_, analysesInColIds) = analyses.foldLeft(initialAcc) { (acc, analysis) =>
      val (mapping, convertedAnalyses) = acc
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.filter { columnName => !mapping.contains(columnName) }
      val mappingWithNewColumns = newlyIntroducedColumns.foldLeft(mapping) { (acc, newColumn) =>
        acc + (newColumn -> new UserColumnId(newColumn.name))
      }
      // Re-map columns except for the innermost soql
      val newMapping =
        if (convertedAnalyses.nonEmpty) {
          val prevAnalysis = convertedAnalyses.last
          prevAnalysis.selection.foldLeft(mapping) { (acc, selCol) =>
            val (colName, expr) = selCol
            acc + (colName -> new UserColumnId(colName.name))
          }
        } else {
          mappingWithNewColumns
        }

      val a: SoQLAnalysis[UserColumnId, SoQLAnalysisType] = analysis.mapColumnIds(newMapping)
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    analysesInColIds
  }

  private def serializeColumn(c: UserColumnId) = c.underlying
  private def deserializeColumn(s: String) = new UserColumnId(s)

  private def serializeAnalysisType(t: SoQLAnalysisType) = t.name.name
  private def deserializeType(s: String): SoQLType = SoQLType.typesByName(TypeName(s))

  private def toAnalysisType(datasetCtx: DatasetContext[SoQLType]): DatasetContext[SoQLAnalysisType] = {
    val soqlAnalysisTypeSchema = datasetCtx.schema.map { case (columnName, soqlType) =>
      (columnName, soqlType.asInstanceOf[SoQLAnalysisType])
    }
    val analysisContext = new DatasetContext[SoQLAnalysisType] {
      val schema = soqlAnalysisTypeSchema
    }
    analysisContext
  }
}
