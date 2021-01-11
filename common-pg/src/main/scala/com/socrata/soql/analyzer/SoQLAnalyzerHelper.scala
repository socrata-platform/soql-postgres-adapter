package com.socrata.soql.analyzer

import java.io.{InputStream, OutputStream}

import com.socrata.NonEmptySeq
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql._
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

  def deserialize(inputStream: InputStream): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = deserializer.applyBinaryTree(inputStream)

  def serializeSeq(outputStream: OutputStream, analyses: NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]]): Unit =
    serializer.apply(outputStream, analyses)

  def deserializeSeq(inputStream: InputStream): NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] =
    deserializer.apply(inputStream)

  private val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def analyzeSoQL(soql: String,
                  datasetCtx: Map[String, DatasetContext[SoQLType]],
                  idMap: Map[QualifiedColumnName, UserColumnId]): NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    val parsed = new Parser().selectStatement(soql)
    val joins = parsed.seq.flatMap(_.joins)

    val joinColumnIdMap =
      joins.foldLeft(idMap) { (acc, join) =>
        val joinTableName = join.from.fromTable
        val joinAlias = join.from.alias.getOrElse(joinTableName.name)
        val schema = datasetCtx(joinTableName.qualifier)
        acc ++ schema.columns.map { fieldName =>
          QualifiedColumnName(Some(joinAlias), new ColumnName(fieldName.name)) ->
            new UserColumnId(fieldName.caseFolded)
        }
      }

    val analyses = analyzer.analyze(parsed)(datasetCtx)
    remapAnalyses(joinColumnIdMap, analyses)
  }

  def analyzeSoQLUnion(soql: String,
                       datasetCtx: Map[String, DatasetContext[SoQLType]],
                       idMap: Map[QualifiedColumnName, UserColumnId]): NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = {

    val ast =  new Parser().test3(soql)
    val parsed = NonEmptySeq(ast)
    val joins = parsed.seq.flatMap(_.joins)

    val joinColumnIdMap =
      joins.foldLeft(idMap) { (acc, join) =>
        val joinTableName = join.from.fromTable
        val joinAlias = join.from.alias.getOrElse(joinTableName.name)
        val schema = datasetCtx(joinTableName.qualifier)
        acc ++ schema.columns.map { fieldName =>
          QualifiedColumnName(Some(joinAlias), new ColumnName(fieldName.name)) ->
            new UserColumnId(fieldName.caseFolded)
        }
      }


    val analysis =  analyzer.analyzeUnion(ast)(datasetCtx) // analyzer.analyze(parsed)(datasetCtx)
    //val analyses = NonEmptySeq(analysis)
    //remapAnalyses(joinColumnIdMap, analyses)
    val analysisr = remapAnalysesUnion(joinColumnIdMap, analysis)
    NonEmptySeq(analysisr)
  }

  def analyzeSoQLBinary(soql: String,
                        datasetCtx: Map[String, DatasetContext[SoQLType]],
                        idMap: Map[QualifiedColumnName, UserColumnId]): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {

    val ast =  new Parser().test(soql)
    //val parsed = NonEmptySeq(ast)
    val joins = ast.seq.flatMap(_.joins)

    val joinColumnIdMap =
      joins.foldLeft(idMap) { (acc, join) =>
        val joinTableName = join.from.fromTable
        val joinAlias = join.from.alias.getOrElse(joinTableName.name)
        val schema = datasetCtx(joinTableName.qualifier)
        acc ++ schema.columns.map { fieldName =>
          QualifiedColumnName(Some(joinAlias), new ColumnName(fieldName.name)) ->
            new UserColumnId(fieldName.caseFolded)
        }
      }

    val analysis = analyzer.analyzeBinary(ast)(datasetCtx)
    val analysisr = remapAnalysesBinary(joinColumnIdMap, analysis)
    analysisr
//
//    val analysis =  analyzer.analyzeUnion(ast)(datasetCtx) // analyzer.analyze(parsed)(datasetCtx)
//    val analysisr = remapAnalysesUnion(joinColumnIdMap, analysis)
//    NonEmptySeq(analysisr)
  }

  /**
   * Remap chained analyses as each analysis may have different selection.
   * This function is for test setup and should mimic the the same function
   * in the "Query Coordinator" project QueryParser class.
   */
  private def remapAnalyses(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                            analyses: NonEmptySeq[SoQLAnalysis[ColumnName, SoQLType]]): NonEmptySeq[SoQLAnalysis[UserColumnId, SoQLType]] = {
    val initialAcc = (columnIdMapping, List.empty[SoQLAnalysis[UserColumnId, SoQLType]])
    val (_, analysesInColIds) = analyses.seq.foldLeft(initialAcc) { (acc, analysis) =>
      val (mapping, convertedAnalyses) = acc
      // Newly introduced columns will be used as column id as is.
      // There should be some sanitizer upstream that checks for field_name conformity.
      // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
      val newlyIntroducedColumns = analysis.selection.keys.map(QualifiedColumnName(None, _)).filter {
        columnName => !mapping.contains(columnName)
      }
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

      val newColumnsFromJoin = analysis.joins.flatMap { join =>
        if (join.isSimple) Seq.empty
        else {
          join.from.analyses.last.selection.toSeq.map {
            case (columnName, _) =>
              QualifiedColumnName(join.from.alias, columnName) -> new UserColumnId(columnName.name)
          }
        }
      }.toMap

      val newMappingWithJoin: Map[(ColumnName, Qualifier), UserColumnId] =
        (newMapping ++ newColumnsFromJoin).map { case (k, v) =>
          (k.columnName, k.qualifier ) -> v
        }

      def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)
      def toUserColumnId(columnName: ColumnName) = new UserColumnId(columnName.name)

      val a: SoQLAnalysis[UserColumnId, SoQLType] = analysis.mapColumnIds(newMappingWithJoin, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
      (mappingWithNewColumns, convertedAnalyses :+ a)
    }
    NonEmptySeq.fromSeqUnsafe(analysesInColIds)
  }

  private def remapAnalysesUnion(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                            analysis: SoQLAnalysis[ColumnName, SoQLType]): SoQLAnalysis[UserColumnId, SoQLType] = {
    val seq = NonEmptySeq(analysis, analysis.more)
    val seqr = remapAnalyses(columnIdMapping, seq)
    seqr.head.copy(op = analysis.op, more = seqr.tail)
  }

  private def remapAnalysesBinaryBad(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                                  btree: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {
    btree.map {
      case x =>
        remapAnalysesUnion(columnIdMapping, x)
    }
  }

  private def remapAnalysesBinary(columnIdMapping: Map[QualifiedColumnName, UserColumnId],
                                    btree: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]]): BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]] = {
    val initialAcc = (columnIdMapping, List.empty[SoQLAnalysis[UserColumnId, SoQLType]])
    btree match {
      case x@Compound(op, l, r) if op == "QUERYPIPE" =>
        //val (_, analysesInColIds) = analyses.seq.foldLeft(initialAcc) { (acc, analysis) =>
          val (mapping, convertedAnalyses) = initialAcc
          // Newly introduced columns will be used as column id as is.
          // There should be some sanitizer upstream that checks for field_name conformity.
          // TODO: Alternatively, we may need to use internal column name map for new and temporary columns

          val lrm = l.rightMost
          val newlyIntroducedColumns = lrm.selection.keys.map(QualifiedColumnName(None, _)).filter {
            columnName => !mapping.contains(columnName)
          }
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

          val newColumnsFromJoin = lrm.joins.flatMap { join =>
            if (join.isSimple) Seq.empty
            else {
              join.from.analyses.last.selection.toSeq.map {
                case (columnName, _) =>
                  QualifiedColumnName(join.from.alias, columnName) -> new UserColumnId(columnName.name)
              }
            }
          }.toMap

        //   case class QualifiedColumnName(qualifier: Option[String], columnName: ColumnName)

          val newMappingWithJoin: Map[QualifiedColumnName, UserColumnId] =
            (newMapping ++ newColumnsFromJoin).map { case (k, v) =>
              //(k.columnName, k.qualifier ) -> v
              QualifiedColumnName(k.qualifier, k.columnName) -> v
            }


          val la = remapAnalysesBinary(columnIdMapping, l)
          val ra = remapAnalysesBinary(newMappingWithJoin, r)
          x.copy(left = la, right = ra)
//          def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)
//          def toUserColumnId(columnName: ColumnName) = new UserColumnId(columnName.name)
//          val a: SoQLAnalysis[UserColumnId, SoQLType] = analysis.mapColumnIds(newMappingWithJoin, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
      //    (mappingWithNewColumns, convertedAnalyses :+ a)
       // }

      case x@Compound(op, l, r) => // if op == "QUERYUNION" =>
        val la = remapAnalysesBinary(columnIdMapping, l)
        val ra = remapAnalysesBinary(columnIdMapping, r)
        x.copy(left = la, right = ra)
      case analysis: SoQLAnalysis[ColumnName, SoQLType] =>
        //val (_, analysesInColIds) = analyses.seq.foldLeft(initialAcc) { (acc, analysis) =>
          val (mapping, convertedAnalyses) = initialAcc
          // Newly introduced columns will be used as column id as is.
          // There should be some sanitizer upstream that checks for field_name conformity.
          // TODO: Alternatively, we may need to use internal column name map for new and temporary columns
          val newlyIntroducedColumns = analysis.selection.keys.map(QualifiedColumnName(None, _)).filter {
            columnName => !mapping.contains(columnName)
          }
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

          val newColumnsFromJoin = analysis.joins.flatMap { join =>
            if (join.isSimple) Seq.empty
            else {
              join.from.analyses.last.selection.toSeq.map {
                case (columnName, _) =>
                  QualifiedColumnName(join.from.alias, columnName) -> new UserColumnId(columnName.name)
              }
            }
          }.toMap

          val newMappingWithJoin: Map[(ColumnName, Qualifier), UserColumnId] =
            (newMapping ++ newColumnsFromJoin).map { case (k, v) =>
              (k.columnName, k.qualifier ) -> v
            }

          def toColumnNameJoinAlias(joinAlias: Option[String], columnName: ColumnName) = (columnName, joinAlias)
          def toUserColumnId(columnName: ColumnName) = new UserColumnId(columnName.name)

          val a: SoQLAnalysis[UserColumnId, SoQLType] = analysis.mapColumnIds(newMappingWithJoin, toColumnNameJoinAlias, toUserColumnId, toUserColumnId)
          (mappingWithNewColumns, convertedAnalyses :+ a)
       // }
        a
    }
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
