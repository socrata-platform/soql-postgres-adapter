package com.socrata.pg.store

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.sql.{Connection, SQLException}
import scala.util.{Failure, Success, Try}
import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.{RollupName, UserColumnId}
import com.socrata.datacoordinator.secondary.{RollupInfo => SecondaryRollupInfo}
import com.socrata.datacoordinator.truth.loader.sql.{ChangeOwner, SqlTableDropper}
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, LifecycleStage}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.error.RowSizeBufferSqlErrorContinue
import com.socrata.pg.soql._
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.store.index.SoQLIndexableRep
import com.socrata.soql.{AnalysisContext, BinaryTree, Compound, Leaf, ParameterSpec, SoQLAnalysis, SoQLAnalyzer}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext, ResourceName, TableName}
import com.socrata.soql.exceptions.{NoSuchColumn, SoQLException}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.standalone_exceptions.{BadParse, StandaloneLexerException}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.typesafe.scalalogging.Logger
import RollupManager.{parseAndCollectTableNames, _}
import com.socrata.datacoordinator.util.{LoggedTimingReport, StackedTimingReport}
import com.socrata.pg.query.QueryServerHelper
import com.socrata.soql.ast.{JoinFunc, JoinQuery, JoinTable, Select}
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.typed.Qualifier

// scalastyle:off multiple.string.literals
class RollupManager(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) extends SecondaryManagerBase(pgu, copyInfo) {
  import RollupManager.logger

  // put rollups in the same tablespace as the copy
  private val tablespaceSql = pgu.commonSupport.tablespace(copyInfo.dataTableName).map(" TABLESPACE " + _).getOrElse("")

  private val dsSchema = getDsSchema(copyInfo)

  private def getDsContext(resourceName: ResourceName) = new DatasetContext[SoQLType] {
    val dsSchemaX = getDsSchema(resourceName)

    // we are sorting by the column name for consistency with query coordinator and how we build
    // schema hashes, it may not matter here though.  Column id to name mapping is 1:1 in our case
    // since our rollup query is pre-mapped.
    val schema: OrderedMap[ColumnName, SoQLType] =
    OrderedMap(dsSchemaX.values.map(x => (columnIdToPrefixNameMap(x.userColumnId), x.typ)).toSeq.sortBy(_._1): _*)
  }

  private val time = new LoggedTimingReport(org.slf4j.LoggerFactory.getLogger("rollup-timing")) with StackedTimingReport

  /**
   * Once we have the analyzed rollup query with column names, we need to remove the leading "_" on non-system
   * columns to make the names match up with the underlying dataset schema.
   * TODO: Join - handle qualifier
   */
  private def columnNameRemovePrefixMap(cn: ColumnName, qualifier: Qualifier): ColumnName = {
    cn.name(0) match {
      case '_' => new ColumnName(cn.name.drop(1))
      case _ => cn // leave system columns (prefixed ':') or derived columns unchanged
    }
  }

  /**
   * Rebuilds the given rollup table, and also schedules the previous version
   * of the rollup table for dropping, if it exists.
   */
  def updateRollup(originalRollupInfo: LocalRollupInfo, oldCopyInfo: Option[CopyInfo], tryToMove: RollupName => Boolean, force: Boolean = false): Unit = {
    var rollupInfo = originalRollupInfo
    time("update-rollup",
         "datasetId" -> copyInfo.datasetInfo.systemId.underlying,
         "oldCopy" -> oldCopyInfo.map(_.copyNumber).getOrElse(0),
         "copy" -> copyInfo.copyNumber,
         "dataVersion" -> copyInfo.dataVersion,
         "shapeVersion" -> copyInfo.dataShapeVersion,
         "rollupName" -> rollupInfo.name.underlying) {
      val oldRollup = oldCopyInfo.flatMap(pgu.datasetMapReader.rollup(_, rollupInfo.name))
      var oldRollupTransferred = false

      // Wrinkle: in unpublished mode, rollups _usually_ don't
      // exist.  We want to update it only if we're forced, in the
      // right lifecycle stage, or we're not changing copy and the
      // table exists.
      lazy val oldRollupExists = oldRollup.exists(rollupTableExists)
      val actuallyUpdateTheTable =
        force ||
          shouldMaterializeRollups(copyInfo.lifecycleStage) ||
          ((oldCopyInfo.map(_.systemId) == Some(copyInfo.systemId)) && oldRollupExists)

      if(actuallyUpdateTheTable) {
        val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
        // In most of the secondary update code, if something unexpectedly blows up we just blow up, roll back
        // the whole transaction, and mark the dataset as broken so we can investigate.  For doing the soql
        // analysis, however, an failure can be caused by user actions, even though the rollup soql is initially
        // validated by soda fountain.  eg. define rollup successfully, then remove a column used in the rollup.
        // We don't want to disable the rollup entirely since it could become valid again, eg. if they then add
        // the column back.  It would be ideal if we had a better way to communicate this failure upwards through
        // the stack.
        val prefixedRollupAnalyses = Try {
          val selects = new StandaloneParser().binaryTreeSelect(rollupInfo.soql)
          val tableNames = collectTableNames(selects)
          val prefixedDsContext0 = Map(TableName.PrimaryTable.qualifier -> new DatasetContext[SoQLType] {
            val schema: OrderedMap[ColumnName, SoQLType] =
              OrderedMap(dsSchema.values.map(x => (columnIdToPrefixNameMap(x.userColumnId), x.typ)).toSeq.sortBy(_._1): _*)
          })
          val prefixedDsSchemas = tableNames.foldLeft(prefixedDsContext0) { (acc, tableName) =>
            val resourceName = ResourceName(tableName)
            val dsctx = getDsContext(resourceName)
            acc + (tableName -> dsctx)
          }
          val prefixedDsContext = AnalysisContext[SoQLType, SoQLValue](
            prefixedDsSchemas,
            ParameterSpec.empty
          )
          analyzer.analyzeBinary(selects)(prefixedDsContext)
        }

        prefixedRollupAnalyses match {
          case Success(pra) =>
            val rollupAnalyses = pra.flatMap(a => Leaf(a.mapColumnIds(columnNameRemovePrefixMap)))
            // We are naming columns simply c1 .. c<n> based on the order they are in to avoid having
            // to maintain a mapping or deal with edge cases such as length and :system columns.
            val rollupReps = rollupAnalyses.last.selection.values.zipWithIndex.map { case (colRep, idx) =>
              SoQLIndexableRep.sqlRep(colRep.typ, "c" + (idx + 1))
            }.toSeq

            oldRollup match {
              case Some(or) if tryToMove(or.name) && oldRollupExists =>
                logger.info("Transferring preexisting rollup table rather than rebuilding it")
                pgu.datasetMapWriter.transferRollup(or, rollupInfo.copyInfo)
                oldRollupTransferred = true
              case _ =>
                // ok, we're going to be making a table.  Which means
                // we don't actually care about the old table and _can
                // change its name_.

                // At this point we are either creating the rollup table for the first time,
                // or we need to rename the existing one and create a new one
                if(oldRollupExists){
                  rollupInfo = pgu.datasetMapWriter.changeRollupTableName(rollupInfo, LocalRollupInfo.tableName(rollupInfo.copyInfo, rollupInfo.name))
                }

                for(or <- oldRollup) {
                  if(or.tableName == rollupInfo.tableName && oldRollupExists) {
                    // This shouldn't happen anymore?  We're mixing in
                    // the data version
                    logger.info("Dropping old rollup with the same name as the new rollup: {}", or.tableName)
                    dropRollup(or, immediate = true) // ick
                  }
                }
                createRollupTable(rollupReps, rollupInfo)
                populateRollupTable(rollupInfo, rollupAnalyses, rollupReps)
                createIndexes(rollupInfo, rollupReps)
            }
          case Failure(e) =>
            e match {
              case e: NoSuchColumn =>
                logger.info(s"drop rollup ${rollupInfo.name.underlying} on ${copyInfo} because ${e.getMessage}")
                dropRollupInfo(rollupInfo)
              case e @ (_:SoQLException | _:StandaloneLexerException | _:BadParse) =>
                logger.warn(s"Error updating ${copyInfo}, ${rollupInfo}, skipping building rollup", e)
              case _ =>
                throw e
            }
        }

        // drop the old rollup regardless so it doesn't leak, because we have no way to use or track old rollups at
        // this point.
        oldRollup.foreach { or =>
          if(or.tableName != rollupInfo.tableName) scheduleRollupTablesForDropping(or.tableName)
          if(or.copyInfo.systemId != rollupInfo.copyInfo.systemId) pgu.datasetMapWriter.dropRollup(or)
        }
      }
    }
  }

  private def rollupTableExists(ri: LocalRollupInfo): Boolean = {
    using(pgu.conn.prepareStatement("SELECT count(*) AS c FROM pg_tables WHERE tablename = ?")) { stmt =>
      stmt.setString(1, ri.tableName)
      using(stmt.executeQuery()) { rs =>
        if(!rs.next()) throw new Exception("SELECT count(*) didn't return any rows???")
        rs.getLong("c") == 1
      }
    }
  }

  private def dropRollupInfo(rollupInfo: LocalRollupInfo) {
    for { ri <- pgu.datasetMapReader.rollup(copyInfo, rollupInfo.name) } {
      dropRollup(ri, immediate = true)
      pgu.datasetMapWriter.dropRollup(copyInfo, Some(rollupInfo.name))
    }
  }

  /**
   * Either immediately drop all rollup tables or put them in pending drop table for the given dataset.
   * Does not update the rollup_map metadata.  The copyInfo passed in
   * must match with the currently created version number of the rollup tables.
   */
  def dropRollups(immediate: Boolean): Unit = {
    val rollups = pgu.datasetMapReader.rollups(copyInfo)
    rollups.foreach(dropRollup(_, immediate))
  }

  /**
   * Immediately drop the specified rollup table.
   * Does not update the rollup_map metadata.  The copyInfo passed in
   * must match with the currently created version number of the rollup table.
   */
  def dropRollup(ri: LocalRollupInfo, immediate: Boolean): Unit = {
    if (immediate) {
      using(pgu.conn.createStatement()) { stmt =>
        val sql = s"DROP TABLE IF EXISTS ${ri.tableName}"
        logger.info(sql)
        stmt.execute(sql)
      }
    } else {
      scheduleRollupTablesForDropping(ri.tableName)
    }
  }

  /**
   * Create the rollup table, but don't add indexes since we want to populate it first.  We are
   * creating it explicitly instead of just doing a CREATE TABLE AS SELECT so we can ensure the
   * column types match what the soql type is and so we can control that, instead of having
   * postgres infer what type to use using its type system.
   */
  private def createRollupTable(rollupReps: Seq[SqlCol], rollupInfo: LocalRollupInfo): Unit = {
    // Note that we aren't doing the work to figure out which columns should be not null
    // or unique since that is of marginal use for us.
    val colDdls = for {
      rep <- rollupReps
      (colName, colType) <- rep.physColumns.zip(rep.sqlTypes)
    } yield s"${colName} ${colType} NULL"

    using(pgu.conn.createStatement()) { stmt =>
      val createSql = s"""CREATE TABLE "${rollupInfo.tableName}" (${colDdls.mkString(", ")} )${tablespaceSql};"""
      time("create-rollup-table",
           "copy" -> copyInfo.copyNumber,
           "dataVersion" -> copyInfo.dataVersion,
           "shapeVersion" -> copyInfo.dataShapeVersion,
           "rollupName" -> rollupInfo.name.underlying,
           "sql" -> createSql) {
        stmt.execute(createSql + ChangeOwner.sql(pgu.conn, rollupInfo.tableName))
        // sadly the COMMENT statement can't use prepared statement params...
        val commentSql = s"""COMMENT ON TABLE "${rollupInfo.tableName}" IS '""" +
          SqlUtils.escapeString(pgu.conn, rollupInfo.name.underlying + " = " + rollupInfo.soql) + "'"
        stmt.execute(commentSql)
      }
    }
  }

  private def scheduleRollupTablesForDropping(tableNames: String*): Unit = {
    using(new SqlTableDropper(pgu.conn)) { dropper =>
      for { tableName <- tableNames } {
        logger.debug(s"Scheduling rollup table ${tableName} for dropping")
        dropper.scheduleForDropping(tableName)
      }
      dropper.go()
    }
  }

  private def populateRollupTable(
    rollupInfo: LocalRollupInfo,
    rollupAnalyses: BinaryTree[SoQLAnalysis[ColumnName, SoQLType]],
    rollupReps: Seq[SqlColumnRep[SoQLType, SoQLValue]]): Unit = {
    val soqlAnalysis = analysesToSoQLType(rollupAnalyses)
    val selectParamSql = QueryServerHelper.sqlize(// scalastyle:ignore method.length parameter.number cyclomatic.complexity
      pgu,
      copyInfo.datasetInfo,
      soqlAnalysis,
      Some(copyInfo.copyNumber.toString), //reqCopy: Option[String],
      None, //rollupName: Option[RollupName],
      false, // obfuscateId: Boolean,
      CaseSensitive,
      true)
    val insertParamSql = selectParamSql.copy(sql = Seq(s"""INSERT INTO "${rollupInfo.tableName}" ( ${selectParamSql.sql.head} )"""))
    time("populate-rollup-table",
         "copy" -> copyInfo.copyNumber,
         "dataVersion" -> copyInfo.dataVersion,
         "shapeVersion" -> copyInfo.dataShapeVersion,
         "rollupName" -> rollupInfo.name.underlying,
         "sql" -> insertParamSql) {
      executeParamSqlUpdate(pgu.conn, insertParamSql)
    }
  }

  private def createIndexes(rollupInfo: LocalRollupInfo, rollupReps: Seq[SqlColIdx]) = {
    time("create-indexes",
         "rollupName" -> rollupInfo.name.underlying) {
      using(pgu.conn.createStatement()) { stmt =>
        for {
          rep <- rollupReps
          createIndexSql <- rep.createRollupIndex(rollupInfo.tableName, tablespaceSql)
        } {
          // Currently we aren't using any SqlErrorHandlers here, because as of this
          // time none of the existing ones are appropriate.
          logger.trace(s"Creating index on ${rollupInfo.tableName} for ${copyInfo} / ${rollupInfo} using sql: ${createIndexSql}")
          RowSizeBufferSqlErrorContinue.guard(pgu.conn) {
            stmt.execute(createIndexSql)
          }
        }
      }
    }
  }

  private def executeParamSqlUpdate(conn: Connection, pSql: ParametricSql): Int = {
    try {
      using(conn.prepareStatement(pSql.sql.head)) { stmt =>
        val stmt = conn.prepareStatement(pSql.sql.head)
        pSql.setParams.zipWithIndex.foreach { case (setParamFn, idx) =>
          setParamFn(Some(stmt), idx + 1)
        }
        stmt.executeUpdate()
      }
    } catch {
      case ex: SQLException =>
        logger.error(s"SQL Exception on ${pSql}")
        throw ex
    }
  }

  private def analysesToSoQLType(analyses: ASysCol): AUserCol = {
    val baos = new ByteArrayOutputStream
    // TODO: Join handle qualifier
    val analysesColumnId = analyses.flatMap(a => Leaf(a.mapColumnIds((name, qualifier) => new UserColumnId(name.name))))
    SoQLAnalyzerHelper.serialize(baos, analysesColumnId)
    SoQLAnalyzerHelper.deserialize(new ByteArrayInputStream(baos.toByteArray))
  }
}

object RollupManager {
  private val logger = Logger[RollupManager]

  def makeSecondaryRollupInfo(rollupInfo: LocalRollupInfo): SecondaryRollupInfo =
     SecondaryRollupInfo(rollupInfo.name.underlying, rollupInfo.soql)

  def shouldMaterializeRollups(stage: LifecycleStage): Boolean = stage == LifecycleStage.Published

  def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
    selects match {
      case Compound(_, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case Leaf(select) =>
        select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
          join.from match {
            case JoinTable(TableName(name, _)) =>
              acc + name
            case JoinQuery(selects, _) =>
              acc ++ collectTableNames(selects)
            case JoinFunc(_, _) =>
              throw new Exception("Unexpected join function")
          }
        }
    }
  }

  def parseAndCollectTableNames(soql: String): Set[String] = collectTableNames(new StandaloneParser().binaryTreeSelect(soql))

  def parseAndCollectTableNames(rollupInfo: LocalRollupInfo):Set[String] = parseAndCollectTableNames(rollupInfo.soql)

}
