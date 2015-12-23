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

  def serialize(outputStream: OutputStream, analysis: SoQLAnalysis[UserColumnId, SoQLAnalysisType]): Unit =
    serializer(outputStream, Seq(analysis))

  def deserialize(inputStream: InputStream): SoQLAnalysis[UserColumnId, SoQLType] = deserializer(inputStream).head

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String,
                  datasetCtx: DatasetContext[SoQLType],
                  idMap: ColumnName => UserColumnId): SoQLAnalysis[UserColumnId, SoQLType] = {
    implicit val ctx: DatasetContext[SoQLAnalysisType] = toAnalysisType(datasetCtx)

    val analysis: SoQLAnalysis[ColumnName, SoQLAnalysisType] = analyzer.analyzeUnchainedQuery(soql)
    val baos = new ByteArrayOutputStream
    serialize(baos, analysis.mapColumnIds(idMap))
    deserialize(new ByteArrayInputStream(baos.toByteArray))
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
