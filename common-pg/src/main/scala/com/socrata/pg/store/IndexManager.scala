package com.socrata.pg.store

import com.rojoma.simplearm.v2.using
import com.socrata.soql.brita.AsciiIdentifierFilter
import com.socrata.datacoordinator.id.{IndexId, IndexName}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, IndexInfo, LifecycleStage}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.error.{SqlErrorHelper, SqlErrorPattern}
import com.socrata.pg.query.QueryResult.QueryRuntimeError
import com.socrata.pg.query.QueryServerHelper.readerWithQuery
import com.socrata.pg.soql.Sqlizer
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.{AnalysisContext, Leaf, ParameterSpec, SoQLAnalyzer}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.ast.{Expression, Indistinct, OrderBy, Select, Selection}
import com.socrata.soql.environment.TableName
import com.socrata.soql.exceptions.NoSuchColumn
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

import java.sql.SQLException

/**
 * This class manages actual creation of custom indexes.
 * Like default indexes and rollups, custom indexes are created for published copies only.
 * Unpublished copies do not have custom db indexes.
 * Update is decomposed to delete and create with index_map.system_id being part of the db index name.
 */
class IndexManager(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) extends SecondaryManagerBase(pgu, copyInfo) {

  import IndexManager._

  val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

  def dbIndexName(index: IndexInfo): String = {
    val base = dbIndexNameLegacy(index) + "_"
    val remainder = (63 - base.length).max(0)
    base + AsciiIdentifierFilter(index.name.underlying).replaceFirst("^_+", "").take(remainder).replaceFirst("_+$", "")
  }

  def dbIndexNameLegacy(index: IndexInfo): String = {
    s"${copyInfo.dataTableName}_idx_${index.systemId.underlying}"
  }

  private def shouldMaterialize(): Boolean = {
    copyInfo.lifecycleStage == LifecycleStage.Published
  }

  def makeSecondaryIndexInfo(indexInfo: IndexInfo): com.socrata.datacoordinator.secondary.IndexInfo  = {
    com.socrata.datacoordinator.secondary.IndexInfo(new IndexId(-1L), indexInfo.name.underlying, indexInfo.expressions, indexInfo.filter)
  }

  def justPublish(start: CopyInfo): Boolean = {
    start.lifecycleStage == LifecycleStage.Unpublished && copyInfo.lifecycleStage == LifecycleStage.Published
  }

  def sqlize(index: IndexInfo) = {
    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
    val parser = new StandaloneParser()
    val exprs = parser.orderings(index.expressions)
    val filter = index.filter.map(parser.expression)
    val dsSchema = getDsSchema(copyInfo)
    val idxName = dbIndexName(index)
    val analyszContext = AnalysisContext[SoQLType, SoQLValue](
      Map(TableName.PrimaryTable.qualifier -> toDatasetContext(dsSchema.values)),
      ParameterSpec.empty
    )
    val select = toSelect(exprs, filter)
    val analysis = analyzer.analyzeWithSelection(select)(analyszContext)
    val cnToCidMap = columnNameToColumnIdMap(dsSchema.values)
    val analysisCid = SoQLAnalyzerHelper.remapAnalyses(cnToCidMap, Leaf(analysis)).outputSchema.leaf
    val coreExprs = analysisCid.orderBys

    for (readCtx <- pgu.datasetReader.openDataset(copyInfo)) {
      val baseSchema: ColumnIdMap[ColumnInfo[SoQLType]] = readCtx.schema
      val systemToUserColumnMap = SchemaUtil.systemToUserColumnMap(readCtx.schema)
      val querier = readerWithQuery(pgu.conn, pgu, readCtx.copyCtx, baseSchema, None)
      val sqlReps = querier.getSqlReps(readCtx.copyInfo.dataTableName, systemToUserColumnMap)
      val typeReps = toTypeRepMap(baseSchema.values)

      val sqlCtx = sqlizeContext + (NameCache -> new Sqlizer.NameCache)

      val psqls = coreExprs.map { expr =>
        Sqlizer.sql(expr)(sqlReps, typeReps, Seq.empty, sqlCtx, escape)
      }

      val psqlsWhere = analysisCid.where.map { expr =>
        Sqlizer.sql(expr)(sqlReps, typeReps, Seq.empty, sqlCtx, escape)
      }

      val setParams = psqls.flatMap(_.setParams) ++ psqlsWhere.toSeq.flatMap(_.setParams)
      assert(setParams.isEmpty)
      val sql = psqls.flatMap(_.sql.headOption).mkString("(", ", ", ")") +
        (if (psqlsWhere.isEmpty) ""
         else psqlsWhere.flatMap(_.sql.headOption).mkString(" WHERE (", "", ")"))
      s"CREATE INDEX IF NOT EXISTS ${idxName} ON ${copyInfo.dataTableName} ${sql}"
    }
  }

  def updateIndex(index: IndexInfo): Unit = {
    if (shouldMaterialize()) {
      try {
        val createIndexSql = sqlize(index)
        try {
          sqlErrorHandler.guard(pgu.conn, None) {
            using(pgu.conn.prepareStatement(createIndexSql)) { stmt =>
              pgu.commonSupport.timingReport.info("create-index",
                "datasetId" -> copyInfo.datasetInfo.systemId.underlying,
                "copyId" -> copyInfo.systemId.underlying,
                "indexName" -> index.name.underlying,
                "sql" -> createIndexSql) {
                stmt.execute()
              }
            }
          }
        } catch {
          case ex: SQLException =>
            QueryRuntimeError(ex) match {
              case None =>
                logger.error(s"SQL Exception create index $createIndexSql (${ex.getSQLState})", ex)
              case Some(e) =>
                logger.warn(s"SQL Exception create index $createIndexSql ${e.getClass.getSimpleName} (${ex.getSQLState})")
            }
        }
      } catch {
        case e: NoSuchColumn =>
          logger.info(s"ignore index ${index.name.underlying} on ${copyInfo} because ${e.getMessage}")
        case e: BadParse =>
          logger.warn(s"ignore index ${index.name.underlying} on ${copyInfo} because ${e.getMessage}")
      }
    }
  }

  def updateIndexes(indexes: Seq[IndexInfo]): Unit = {
    if (indexes.isEmpty) {
      updateIndexes()
    } else {
      for (index <- indexes) updateIndex(index)
    }
  }

  def updateIndexes(): Unit = {
    if (shouldMaterialize()) {
      pgu.datasetMapReader.indexes(copyInfo).foreach { index =>
        updateIndex(index)
      }
    }
  }

  def dropIndex(index: IndexInfo): Unit = {
    if (shouldMaterialize()) {
      val createSql = s"INSERT INTO pending_index_drops(name) VALUES(?),(?)"
      using(pgu.conn.prepareStatement(createSql)) { stmt =>
        // tell it to drop the old-style name and the new-style name -
        // we only care about index names at creation and deletion
        // time, and we don't store them in the database, so we have a
        // bunch of old-style indexes whose name is in the "legacy"
        // format - which is the same as the existing format minus the
        // "human-name" suffix.
        stmt.setString(1, dbIndexName(index))
        stmt.setString(2, dbIndexNameLegacy(index))
        stmt.execute()
      }
    }
  }

  def dropIndex(indexName: IndexName): Unit = {
    if (shouldMaterialize()) {
      pgu.datasetMapReader.index(copyInfo, indexName).foreach(dropIndex)
    }
  }
}

object IndexManager {
  private val logger = Logger[IndexManager]

  val sqlizeContext = Map[SqlizerContext, Any](
    TableAliasMap -> Map.empty,
    SimpleJoinMap -> Map.empty,
    InnermostSoql -> true,
    SoqlPart -> SoqlGroup /* Do not use parameters for literals because parameters are not supported in DDL.
    All variable parts are internally generated and not related to user input */
  )

  def toSelect(exprs: Seq[OrderBy], filter: Option[Expression]): Select = {
    Select(distinct = Indistinct,
      selection = Selection(None, Nil, Nil),
      from = None,
      joins = Nil,
      where = filter,
      groupBys = Nil,
      having = None,
      orderBys = exprs,
      limit = None,
      offset = None,
      search = None,
      hints = Nil)
  }

  private val sqlErrorPatterns = QueryRuntimeError.validErrorCodes.map(state => SqlErrorPattern(state, ".*".r)).toSeq
  val sqlErrorHandler = new SqlErrorHelper(sqlErrorPatterns: _*)
}
