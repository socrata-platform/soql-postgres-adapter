package com.socrata.pg.store

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.IndexName
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, IndexInfo, LifecycleStage}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.query.QueryServerHelper.readerWithQuery
import com.socrata.pg.soql.Sqlizer
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.{Leaf, SoQLAnalyzer}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.ast.{Expression, OrderBy, Select, Selection}
import com.socrata.soql.environment.TableName
import com.socrata.soql.exceptions.NoSuchColumn
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.parsing.standalone_exceptions.BadParse
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger

import java.sql.SQLException

class IndexManager(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) extends SecondaryManagerBase(pgu, copyInfo) {

  import IndexManager._

  val escape = (stringLit: String) => SqlUtils.escapeString(pgu.conn, stringLit)

  def dbIndexName(index: IndexInfo): String = {
    s"${copyInfo.dataTableName}_idx_${index.systemId.underlying}"
  }

  private def shouldMaterialize(): Boolean = {
    copyInfo.lifecycleStage == LifecycleStage.Published
  }

  def sqlize(index: IndexInfo) = {
    val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
    val parser = new StandaloneParser()
    val exprs = parser.orderings(index.expressions)
    val filter = index.filter.map(parser.expression)
    val dsSchema = getDsSchema(copyInfo)
    val idxname = dbIndexName(index)
    val analyszContext = Map(TableName.PrimaryTable.qualifier -> toDatasetContext(dsSchema.values))
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
      val psqls = coreExprs.map { expr =>
        Sqlizer.sql(expr)(sqlReps, typeReps, Seq.empty, sqlizeContext, escape)
      }

      val psqlsWhere = analysisCid.where.map { expr =>
        Sqlizer.sql(expr)(sqlReps, typeReps, Seq.empty, sqlizeContext, escape)
      }

      val setParams = psqls.flatMap(_.setParams) ++ psqlsWhere.toSeq.flatMap(_.setParams)
      assert(setParams.isEmpty)
      val sql = psqls.flatMap(_.sql.headOption).mkString("(", ", ", ")") +
        (if (psqlsWhere.isEmpty) ""
        else psqlsWhere.flatMap(_.sql.headOption).mkString(" WHERE (", "", ")"))
      s"CREATE INDEX IF NOT EXISTS ${idxname} ON ${copyInfo.dataTableName} ${sql}"
    }
  }

  def updateIndex(index: IndexInfo): Unit = {
    if (shouldMaterialize()) {
      try {
        val createIndexSql = sqlize(index)
        using(pgu.conn.prepareStatement(createIndexSql)) { stmt =>
          pgu.commonSupport.timingReport.info("create-index",
            "datasetId" -> copyInfo.datasetInfo.systemId.underlying,
            "copyId" -> copyInfo.systemId.underlying,
            "indexName" -> index.name.underlying,
            "sql" -> createIndexSql) {
            stmt.execute()
          }
        }
      } catch {
        case e: NoSuchColumn =>
          logger.info(s"ignore index ${index.name.underlying} on ${copyInfo} because ${e.getMessage}")
        case e: BadParse =>
          logger.warn(s"ignore index ${index.name.underlying} on ${copyInfo} because ${e.getMessage}")
        case e: SQLException =>
          logger.warn(s"bad index sql", e)
          pgu.conn.clearWarnings()
          throw e
      }
    }
  }

  def updateIndexes(indexes: Seq[IndexInfo]): Unit = {
    if (indexes.isEmpty) {
      updateIndexes(true)
    } else {
      for (index <- indexes) updateIndex(index)
    }
  }

  def updateIndexes(dropIndexBeforeUpdate: Boolean): Unit = {
    if (shouldMaterialize()) {
      pgu.datasetMapReader.indexes(copyInfo).foreach { index =>
        if (dropIndexBeforeUpdate) {
          dropIndex(index)
        }
        updateIndex(index)
      }
    }
  }

  def dropIndex(index: IndexInfo): Unit = {
    if (shouldMaterialize()) {
      val createSql = s"INSERT INTO pending_index_drops(name) VALUES(?)"
      using(pgu.conn.prepareStatement(createSql)) { stmt =>
        val idxname = dbIndexName(index)
        stmt.setString(1, idxname)
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
    SoqlPart -> SoqlGroup /* Do not use parameters for literals, parameters are not supported in DDL */
  )

  def toSelect(exprs: Seq[OrderBy], filter: Option[Expression]): Select = {
    Select(distinct = false,
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
}
