package com.socrata.soql.analyzer

import java.io.{InputStream, OutputStream}

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql._
import com.socrata.soql.ast.TableName
import com.socrata.soql.environment._
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLFunctions, SoQLTypeInfo}
import com.socrata.soql.parsing.Parser
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLType

object SoQLAnalyzerHelper {
  private val serializer: AnalysisSerializer[UserColumnId, SoQLType] =
    new AnalysisSerializer(serializeColumn, serializeAnalysisType)

  private val deserializer: AnalysisDeserializer[UserColumnId, SoQLType] =
    new AnalysisDeserializer(deserializeColumn,
      deserializeType,
      SoQLFunctions.functionsByIdentity)

  def serialize(outputStream: OutputStream, analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]]): Unit =
    serializer.applyBinaryTree(outputStream, analyses)

  def deserialize(inputStream: InputStream): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] =
    deserializer.applyBinaryTree(inputStream)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String,
                  datasetCtx: Map[String, DatasetContext[SoQLType]],
                  idMap: Map[QualifiedColumnName, UserColumnId]): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {

    val ast =  new Parser().binaryTreeSelect(soql)
    val analysis = analyzer.analyzeBinary(ast)(datasetCtx)
    remapAnalyses(idMap, analysis)
  }

  /**
   * Remap chained analyses as each analysis may have different selection.
   * This function is for test setup and should mimic the the same function
   * in the "Query Coordinator" project QueryParser class.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                            analyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]):
      BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {

    val newMapping: Map[(ColumnName, Qualifier), UserColumnId] = columnIdMapping map {
      case (QualifiedColumnName(qualifier, columnName), userColumnId) =>
        ((columnName, qualifier), userColumnId)
    }

    def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)
    def toUserColumnId(columnName: ColumnName) = new UserColumnId(columnName.name)

    analyses match {
      case PipeQuery(l, r) =>
        val nl = remapAnalyses(columnIdMapping, l)
        val prev = nl.outputSchema.leaf
        val ra = r.asLeaf.get
        val prevQueryAlias = ra.from match {
          case Some(TableName(TableName.This, alias@Some(_), _)) =>
            alias
          case _ =>
            None
        }
        val prevQColumnIdToQColumnIdMap = prev.selection.foldLeft(newMapping) { (acc, selCol) =>
          val (colName, _expr) = selCol
          acc + ((colName, prevQueryAlias) -> toUserColumnId(colName))
        }
        val nr = r.asLeaf.get.mapColumnIds(prevQColumnIdToQColumnIdMap, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
        PipeQuery(nl, Leaf(nr))
      case Compound(op, l, r) =>
        val la = remapAnalyses(columnIdMapping, l)
        val ra = remapAnalyses(columnIdMapping, r)
        Compound(op, la, ra)
      case Leaf(analysis) =>
        val newMappingThisAlias = analysis.from match {
          case Some(tn@TableName(TableName.This, Some(_), _)) =>
            newMapping.foldLeft(newMapping) { (acc, mapEntry) =>
              mapEntry match {
                case ((columnName, None), userColumnId) =>
                  acc ++ Map((columnName, Some(tn.qualifier)) -> userColumnId,
                             (columnName, Some(TableName.This)) -> userColumnId)
                case _ =>
                  acc
              }
            }
          case _ =>
            newMapping
        }

        val remapped: SoQLAnalysis[UserColumnId, SoQLType] = analysis.mapColumnIds(newMappingThisAlias, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
        Leaf(remapped)
    }
  }

  private def serializeColumn(c: UserColumnId) = c.underlying
  private def deserializeColumn(s: String) = new UserColumnId(s)

  private def serializeAnalysisType(t: SoQLType) = t.name.name
  private def deserializeType(s: String): SoQLType = SoQLType.typesByName(TypeName(s))
}
