package com.socrata.soql.analyzer

import java.io.{InputStream, OutputStream}

import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.json.v3.ast.{JArray, JString}

import com.socrata.soql.collection.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql._
import com.socrata.soql.ast.Select
import com.socrata.soql.environment._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType

object SoQLAnalyzerHelper {
  private val serializer: AnalysisSerializer[UserColumnId, Qualified[UserColumnId], SoQLType] =
    new AnalysisSerializer(serializeColumn, serializeQualifiedColumn, serializeAnalysisType)

  private val deserializer: AnalysisDeserializer[UserColumnId, Qualified[UserColumnId], SoQLType] =
    new AnalysisDeserializer(deserializeColumn,
      deserializeQualifiedColumn,
      deserializeType,
      SoQLFunctions.functionsByIdentity)

  def serialize(outputStream: OutputStream, analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, Qualified[UserColumnId], SoQLType]]): Unit =
    serializer(outputStream, analyses)

  def deserialize(inputStream: InputStream): NonEmptySeq[SoQLAnalysis[UserColumnId, Qualified[UserColumnId], SoQLType]] = deserializer(inputStream)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  case class SqlSafeColumnId(name: ColumnName, ucid: UserColumnId)

  trait AugmentedDatasetContext extends DatasetContext[SoQLType] {
    val columnIds: Map[ColumnName, UserColumnId]
  }

  def analyzeSoQL(resourceName: ResourceName,
                  soql: String,
                  datasetCtx: Set[ResourceName] => Map[ResourceName, AugmentedDatasetContext]): NonEmptySeq[SoQLAnalysis[SqlSafeColumnId, Qualified[SqlSafeColumnId], SoQLType]] = {
    val parsed = new Parser().selectStatement(soql)
    val tables = datasetCtx(Select.allTableReferences(parsed) + resourceName)
    val analysis = analyzer.analyze(resourceName, tables, parsed)
    // ok so that analysis is entirely in terms of _field names_ and
    // we want them to be in terms of _user column ids_, where "ucid"
    // is something we make up.
    //
    // Since we don't want user-specified text ending up the the
    // query, we'll map over the column references to convert.
    // Because all queries are qualified, we don't care if we re-use
    // the same UCID for columns coming from two different soures, so
    // the names that don't correspond to physical tables (the ones
    // that are either unqualified or have a non-Primary-like
    // qualification) we'll just use a map from field name to a
    // counter.  So for example @x.a and @y.a might both map to
    // synthetic column #17 but we don't care because we have the
    // prefix to disambiguate.
    val (_, converted) = SoQLAnalysis.mapAccumColumnIds(analysis, Map.empty[ColumnName, Int]) {
      new ColumnIdTransformAccum[Map[ColumnName, Int], ColumnName, Qualified[ColumnName], SqlSafeColumnId, Qualified[SqlSafeColumnId]]  {
        def mapColumnId(state: Map[ColumnName, Int], cid: ColumnName) =
          state.get(cid) match {
            case Some(i) =>
              (state, SqlSafeColumnId(cid, new UserColumnId("intermediate_" + i)))
            case None =>
              (state + (cid -> state.size),
               SqlSafeColumnId(cid,
                               new UserColumnId("intermediate_" + state.size)))
          }
        def mapQualifiedColumnId(state: Map[ColumnName, Int], cid: Qualified[ColumnName]) =
          cid.table match {
            case TableRef.Primary =>
              (state, cid.copy(columnName = SqlSafeColumnId(cid.columnName,
                                                            tables(resourceName).columnIds(cid.columnName))))
            case TableRef.JoinPrimary(rn, _) =>
              (state, cid.copy(columnName = SqlSafeColumnId(cid.columnName,
                                                            tables(rn).columnIds(cid.columnName))))
            case _ =>
              val (newState, newCname) = mapColumnId(state, cid.columnName)
              (newState, cid.copy(columnName = newCname))
          }
      }
    }
    converted
  }

  private def qualifiedColumnNameToColumnId[T](qualifiedColumnNameMap: Map[QualifiedColumnName, T])
                                              (columnName: ColumnName, qualifier: Option[String]): T = {
    qualifiedColumnNameMap(QualifiedColumnName(qualifier, columnName))
  }

  private def serializeColumn(c: UserColumnId) = c.underlying
  private def deserializeColumn(s: String) = new UserColumnId(s)

  private def serializeQualifiedColumn(qc: Qualified[UserColumnId]) = {
    val Qualified(q, c) = qc
    CompactJsonWriter.toString(JArray(Seq(JString(TableRef.serialize(q)),
                                          JString(c.underlying))))
  }

  private def deserializeQualifiedColumn(s: String) = {
    JsonReader.fromString(s) match {
      case JArray(Seq(JString(q), JString(c))) =>
        Qualified(TableRef.deserialize(q).get, new UserColumnId(c))
      case _ =>
        throw new Exception("Can't decode a qualified column Id?")
    }
  }

  private def serializeAnalysisType(t: SoQLType) = t.name.name
  private def deserializeType(s: String): SoQLType = SoQLType.typesByName(TypeName(s))
}
