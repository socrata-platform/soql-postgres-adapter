package com.socrata.pg.query

import com.socrata.datacoordinator.common.soql.SoQLTypeContext
import com.socrata.datacoordinator.id.{ColumnId, RollupName, UserColumnId}
import com.socrata.datacoordinator.truth.loader.sql.PostgresRepBasedDataSqlizer
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, DatasetCopyContext, DatasetInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.soql.{CaseSensitivity, ParametricSql, QualifiedUserColumnId, Sqlizer, SqlizerContext}
import com.socrata.pg.store.{PGSecondaryRowReader, PGSecondaryUniverse, SchemaUtil, SqlUtils}
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, ResourceName, TableName}
import com.socrata.soql.{BinaryTree, SoQLAnalysis, typed}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.SoQLID.ClearNumberRep
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.types.{SoQLID, SoQLType, SoQLValue, SoQLVersion}

import java.sql.Connection

object QueryServerHelper {

  /**
   * TODO: Refactor QueryServer.runQuery to use this function
   */
  def sqlize(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
             datasetInfo: DatasetInfo,
             analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
             reqCopy: Option[String],
             rollupName: Option[RollupName],
             obfuscateId: Boolean,
             caseSensitivity: CaseSensitivity,
             leadingSearch: Boolean = true): ParametricSql = {
    val copy = getCopy(pgu, datasetInfo, reqCopy)
    val cryptProvider = new CryptProvider(datasetInfo.obfuscationKey)

    val sqlCtx = Map[SqlizerContext, Any](
      SqlizerContext.IdRep -> (if (obfuscateId) { new SoQLID.StringRep(cryptProvider) }
                               else { new ClearNumberRep(cryptProvider) }),
      SqlizerContext.VerRep -> new SoQLVersion.StringRep(cryptProvider),
      SqlizerContext.CaseSensitivity -> caseSensitivity,
      SqlizerContext.LeadingSearch -> leadingSearch
    )
    val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

    val lma = analyses.leftMost.leaf

    for(readCtx <- pgu.datasetReader.openDataset(copy)) {
      val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
      val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val querier = this.readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, rollupName)
      val sqlReps = querier.getSqlReps(readCtx.copyInfo.dataTableName, systemToUserColumnMap)

      // TODO: rethink how reps should be constructed and passed into each chained soql.
      val typeReps = analyses.seq.flatMap { analysis =>
        val qrySchema = querySchema(pgu, analysis, copy)
        qrySchema.map { case (columnId, columnInfo) =>
          columnInfo.typ -> pgu.commonSupport.repFor(columnInfo)
        }
      }.toMap

      // rollups will cause querier's dataTableName to be different than the normal dataset tablename
      val tableName = querier.sqlizer.dataTableName
      val copyInfo = readCtx.copyInfo
      val joinCopiesMap = getJoinCopies(pgu, analyses, reqCopy) ++ copyInfo.datasetInfo.resourceName.map(rn => Map(TableName(rn) -> copyInfo)).getOrElse(Map.empty)

      val sqlRepsWithJoin = joinCopiesMap.foldLeft(sqlReps) { (acc, joinCopy) =>
        val (tableName, copyInfo) = joinCopy
        acc ++ getJoinReps(pgu, copyInfo, tableName)
      }

      val thisTableNameMap = lma.from match {
        case Some(tn@(TableName(name, _))) if name == TableName.This =>
          Map(tn.copy(alias = None) -> tableName)
        case _ =>
          Map.empty
      }

      val tableNameMap = getDatasetTablenameMap(joinCopiesMap) ++ thisTableNameMap +
        (TableName.PrimaryTable -> tableName) + (TableName(tableName) -> tableName)
      Sqlizer.sql((analyses, tableNameMap, sqlReps.values.toSeq))(sqlRepsWithJoin, typeReps, Seq.empty, sqlCtx, escape)
    }
  }

  def readerWithQuery[SoQLType, SoQLValue](conn: Connection,
                                           pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                                           copyCtx: DatasetCopyContext[SoQLType],
                                           schema: ColumnIdMap[ColumnInfo[SoQLType]],
                                           rollupName: Option[RollupName]):
    PGSecondaryRowReader[SoQLType, SoQLValue] with RowReaderQuerier[SoQLType, SoQLValue] = {

    val tableName = rollupName match {
      case Some(r) =>
        val rollupInfo = pgu.datasetMapReader.rollup(copyCtx.copyInfo, r).getOrElse {
          throw new RuntimeException(s"Rollup ${rollupName} not found for copy ${copyCtx.copyInfo} ")
        }
        rollupInfo.tableName
      case None =>
        copyCtx.copyInfo.dataTableName
    }

    new PGSecondaryRowReader[SoQLType, SoQLValue] (
      conn,
      new PostgresRepBasedDataSqlizer(
        tableName,
        pgu.datasetContextFactory(schema),
        pgu.commonSupport.copyInProvider
      ) with DataSqlizerQuerier[SoQLType, SoQLValue],
      pgu.commonSupport.timingReport
    ) with RowReaderQuerier[SoQLType, SoQLValue]
  }

  def getJoinCopies(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                    analyses: BinaryTree[SoQLAnalysis[UserColumnId, SoQLType]],
                    reqCopy: Option[String]): Map[TableName, CopyInfo] = {
    val joins = typed.Join.expandJoins(analyses.seq)
    val froms = analyses.seq.flatMap(x => x.from.toSeq)
    val joinTables = joins.flatMap(x => x.from.fromTables) ++ froms
    val joinTableMap = joinTables.flatMap { resourceName =>
      getCopy(pgu, new ResourceName(resourceName.name), reqCopy).map(copy => (resourceName, copy))
    }.toMap

    val allMap = joinTableMap
    val allMapNoAliases = allMap.map {
      case (tableName, copy) =>
        (tableName.copy(alias = None), copy)
    }

    allMapNoAliases
  }

  def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], ds: String, reqCopy: Option[String]): Option[CopyInfo] = {
    for {
      datasetId <- pgu.datasetMapReader.datasetIdForInternalName(ds, checkDisabled = true)
      datasetInfo <- pgu.datasetMapReader.datasetInfo(datasetId)
    } yield {
      getCopy(pgu, datasetInfo, reqCopy)
    }
  }

  def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], datasetInfo: DatasetInfo, reqCopy: Option[String]): CopyInfo = {
    val intRx = "^[0-9]+$".r
    val rd = pgu.datasetMapReader
    reqCopy match {
      case Some("latest") =>
        rd.latest(datasetInfo)
      case Some("published") | None =>
        rd.published(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some("unpublished") | None =>
        rd.unpublished(datasetInfo).getOrElse(rd.latest(datasetInfo))
      case Some(intRx(num)) =>
        rd.copyNumber(datasetInfo, num.toLong).getOrElse(rd.latest(datasetInfo))
      case Some(unknown) =>
        throw new IllegalArgumentException(s"invalid copy value $unknown")
    }
  }

  def getCopy(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], resourceName: ResourceName, reqCopy: Option[String]): Option[CopyInfo] = {
    for {
      datasetInfo <- pgu.datasetMapReader.datasetInfoByResourceName(resourceName)
    } yield {
      getCopy(pgu, datasetInfo, reqCopy)
    }
  }

  def getDatasetTablenameMap(copies: Map[TableName, CopyInfo]): Map[TableName, String] = {
    copies.mapValues { copy =>
      copy.dataTableName
    }
  }

  def getJoinReps(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                  copy: CopyInfo,
                  tableName: TableName): Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]] = {
    for(readCtx <- pgu.datasetReader.openDataset(copy)) {
      val schema = readCtx.schema
      val columnIdUserColumnIdMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val columnIdReps = schema.mapValuesStrict(pgu.commonSupport.repFor)
      columnIdReps.keys.map { columnId =>
        val userColumnId: UserColumnId = columnIdUserColumnIdMap(columnId)
        val qualifier = tableName.alias.orElse(copy.datasetInfo.resourceName)
        val qualifiedUserColumnId = new QualifiedUserColumnId(qualifier, userColumnId)
        qualifiedUserColumnId -> columnIdReps(columnId)
      }.toMap
    }
  }

  /**
   * @param pgu
   * @param analysis parsed soql
   * @param latest
   * @return a schema for the selected columns
   */
  // TODO: Handle expressions and column aliases.
  def querySchema(pgu: PGSecondaryUniverse[SoQLType, SoQLValue],
                  analysis: SoQLAnalysis[UserColumnId, SoQLType],
                  latest: CopyInfo): OrderedMap[ColumnId, ColumnInfo[pgu.CT]] = {
    analysis.selection.foldLeft(OrderedMap.empty[ColumnId, ColumnInfo[pgu.CT]]) { (map, entry) =>
      entry match {
        case (columnName: ColumnName, coreExpr: CoreExpr[UserColumnId, SoQLType]) =>
          val cid = new ColumnId(map.size + 1)
          val cinfo = new ColumnInfo[pgu.CT](
            latest,
            cid,
            new UserColumnId(columnName.name),
            None, // field name
            coreExpr.typ,
            columnName.name,
            coreExpr.typ == SoQLID,
            false, // isUserKey
            coreExpr.typ == SoQLVersion,
            None, // computation strategy we aren't actually storing...
            Seq.empty
          )(SoQLTypeContext.typeNamespace, null) // scalastyle:ignore null
          map + (cid -> cinfo)
      }
    }
  }
}
