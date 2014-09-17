package com.socrata.pg.store

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.MessageDigest
import java.sql.{Connection, SQLException}

import com.rojoma.simplearm.util.using
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.secondary.{RollupInfo => SecondaryRollupInfo}
import com.socrata.datacoordinator.truth.loader.sql.SqlTableDropper
import com.socrata.datacoordinator.truth.metadata.{ColumnInfo, CopyInfo, RollupInfo}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.pg.soql.SqlizerContext.SqlizerContext
import com.socrata.pg.soql.{ParametricSql, SoQLAnalysisSqlizer, SqlizerContext}
import com.socrata.pg.store.index.{Indexable, SoQLIndexableRep}
import com.socrata.soql.analyzer.SoQLAnalyzerHelper
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.exceptions.SoQLException
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.parsing.standalone_exceptions.StandaloneLexerException
import com.socrata.soql.types.{SoQLAnalysisType, SoQLType, SoQLValue}
import com.socrata.soql.{SoQLAnalysis, SoQLAnalyzer}
import com.typesafe.scalalogging.slf4j.Logging

import scala.util.{Failure, Success, Try}

class RollupManager(pgu: PGSecondaryUniverse[SoQLType, SoQLValue], copyInfo: CopyInfo) extends Logging {
  import RollupManager._

  // put rollups in the same tablespace as the copy
  private val tablespaceSql = pgu.commonSupport.tablespace(copyInfo.dataTableName).map(" TABLESPACE " + _).getOrElse("")

  private val dsSchema: ColumnIdMap[ColumnInfo[SoQLType]] = for {
    readCtx <- pgu.datasetReader.openDataset(copyInfo)
  } yield (readCtx.schema)

  private val dsContext = new DatasetContext[SoQLAnalysisType] {
    // we are sorting by the column name for consistency with query coordinator and how we build
    // schema hashes, it may not matter here though.  Column id to name mapping is 1:1 in our case
    // since our rollup query is pre-mapped.
    val schema: OrderedMap[ColumnName, SoQLAnalysisType] =
      OrderedMap((dsSchema.values.map(x => (ColumnName(x.userColumnId.underlying), x.typ))).toSeq.sortBy(_._1): _*)
  }

  private val time = pgu.commonSupport.timingReport

  /**
   * For analyzing the rollup query, we need to map the dataset schema column ids to the "_" prefixed
   * version of the name that we get, designed to ensure the column name is valid soql
   * and doesn't start with a number.
   */
  private def columnIdToPrefixNameMap(cid: UserColumnId): ColumnName = {
    val name = cid.underlying
    new ColumnName(if (name(0) == ':') name else "_" + name)
  }

  /**
   * Once we have the analyzed rollup query with column names, we need to remove the leading "_" on non-system
   * columns to make the names match up with the underlying dataset schema.
   */
  private def columnNameRemovePrefixMap(cn: ColumnName): ColumnName = {
    cn.name(0) match {
      case ':' => cn
      case _ => new ColumnName(cn.name.drop(1))
    }
  }

  /**
   * Rebuilds the given rollup table, and also schedules the previous version
   * of the rollup table for dropping, if it exists.
   *
   * @param newDataVersion The version of the dataset that will be current after the current
   *                       transaction completes.
   */
  def updateRollup(rollupInfo: RollupInfo, newDataVersion: Long) {
    logger.info(s"Updating copy ${copyInfo}, rollup ${rollupInfo}")
    time("update-rollup", "dataset_id" -> copyInfo.datasetInfo.systemId, "rollupName" -> rollupInfo.name) {

      val newTableName = rollupTableName(rollupInfo, newDataVersion)
      val oldTableName = rollupTableName(rollupInfo, newDataVersion - 1)

      val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

      val prefixedDsContext = new DatasetContext[SoQLAnalysisType] {
        val schema: OrderedMap[ColumnName, SoQLAnalysisType] =
          OrderedMap((dsSchema.values.map(x => (columnIdToPrefixNameMap(x.userColumnId), x.typ))).toSeq.sortBy(_._1): _*)
      }

      // In most of the secondary update code, if something unexpectedly blows up we just blow up, roll back
      // the whole transaction, and mark the dataset as broken so we can investigate.  For doing the soql
      // analysis, however, an failure can be caused by user actions, even though the rollup soql is initially
      // validated by soda fountain.  eg. define rollup successfully, then remove a column used in the rollup.
      // We don't want to disable the rollup entirely since it could become valid again, eg. if they then add
      // the column back.  It would be ideal if we had a better way to communicate this failure upwards through
      // the stack.
      val prefixedRollupAnalysis: Try[SoQLAnalysis[ColumnName, SoQLAnalysisType]] =
        Try (analyzer.analyzeFullQuery(rollupInfo.soql)(prefixedDsContext))


      prefixedRollupAnalysis match {
        case Success(pra) =>
          val rollupAnalysis = pra.mapColumnIds(columnNameRemovePrefixMap)
          // We are naming columns simply c1 .. c<n> based on the order they are in to avoid having to maintain a mapping or
          // deal with edge cases such as length and :system columns.
          val rollupReps = rollupAnalysis.selection.values.zipWithIndex.map { case (colRep, idx) =>
              SoQLIndexableRep.sqlRep(colRep.typ.canonical, "c" + (idx + 1))
            }.toSeq

          createRollupTable(rollupReps, newTableName, rollupInfo)
          populateRollupTable(newTableName, rollupInfo, rollupAnalysis, rollupReps)
          createIndexes(newTableName, rollupInfo, rollupReps)
        case e @ (Failure(_:SoQLException) | Failure(_:StandaloneLexerException)) =>
          logger.warn(s"Error updating ${copyInfo}, ${rollupInfo}, skipping building rollup", e)
        case Failure(e) =>
          throw e
      }

      // drop the old rollup regardless so it doesn't leak, because we have no way to use or track old rollups at
      // this point.
      scheduleRollupTablesForDropping(oldTableName)
    }
  }

  /**
   * Immediately drop all rollup tables for the given dataset.
   * Does not update the rollup_map metadata.  The copyInfo passed in
   * must match with the currently created version number of the rollup tables.
   */
  def dropRollups() {
    val rollups = pgu.datasetMapReader.rollups(copyInfo)
    rollups.foreach(dropRollup)
  }

  /**
   * Immediately drop the specified rollup table.
   * Does not update the rollup_map metadata.  The copyInfo passed in
   * must match with the currently created version number of the rollup table.
   */
  def dropRollup(ri: RollupInfo) {
    val tableName = rollupTableName(ri, copyInfo.dataVersion)
    using(pgu.conn.createStatement()) { stmt =>
      val sql = s"DROP TABLE IF EXISTS ${tableName}"
      logger.info(sql)
      stmt.execute(sql)
    }
  }

  /**
   * Create the rollup table, but don't add indexes since we want to populate it first.  We are
   * creating it explicitly instead of just doing a CREATE TABLE AS SELECT so we can ensure the
   * column types match what the soql type is and so we can control that, instead of having
   * postgres infer what type to use using its type system.
   */
  private def createRollupTable(rollupReps: Seq[SqlColumnRep[SoQLType, SoQLValue]], tableName: String, rollupInfo: RollupInfo) {
    time("create-rollup-table", "dataset_id" -> copyInfo.datasetInfo.systemId.underlying, "rollupName" -> rollupInfo.name.underlying) {
      // Note that we aren't doing the work to figure out which columns should be not null
      // or unique since that is of marginal use for us.
      val colDdls = for {
        rep <- rollupReps
        (colName, colType) <- rep.physColumns.zip(rep.sqlTypes)
      } yield (s"${colName} ${colType} NULL")

      using(pgu.conn.createStatement()) { stmt =>
        val createSql = s"CREATE TABLE ${tableName} (${colDdls.mkString(", ")} )${tablespaceSql}"
        logger.info(s"Creating rollup table ${tableName} for ${copyInfo} / ${rollupInfo} using sql: ${createSql}")
        stmt.execute(createSql)

        // sadly the COMMENT statement can't use prepared statement params...
        val commentSql = s"COMMENT ON TABLE ${tableName} IS '" +
          SqlUtils.escapeString(pgu.conn, rollupInfo.name.underlying + " = " + rollupInfo.soql) + "'"
        stmt.execute(commentSql)
      }
    }
  }

  private def scheduleRollupTablesForDropping(tableNames: String*) {
    using(new SqlTableDropper(pgu.conn)) { dropper =>
      for (tableName <- tableNames) {
        logger.debug(s"Scheduling rollup table ${tableName} for dropping")
        dropper.scheduleForDropping(tableName)
      }
      dropper.go()
    }
  }

  private def populateRollupTable(tableName: String,
      rollupInfo: RollupInfo,
      rollupAnalysis: SoQLAnalysis[ColumnName, SoQLAnalysisType],
      rollupReps: Seq[SqlColumnRep[SoQLType, SoQLValue]])
  {
    time("populate-rollup-table", "dataset_id" -> copyInfo.datasetInfo.systemId.underlying, "rollupName" -> rollupInfo.name.underlying) {
      val soqlAnalysis = analysisToSoQLType(rollupAnalysis)
      val sqlizer = new SoQLAnalysisSqlizer(soqlAnalysis, copyInfo.dataTableName, rollupReps)
      val sqlCtx = Map[SqlizerContext, Any](
        SqlizerContext.CaseSensitivity -> true
      )

      val dsRepMap: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]] =
        dsSchema.values.map(ci => ci.userColumnId -> SoQLIndexableRep.sqlRep(ci)).toMap

      val selectParamSql = sqlizer.sql(
        rep = dsRepMap,
        setParams = Seq(),
        ctx = sqlCtx)

      val insertParamSql = selectParamSql.copy(sql = s"INSERT INTO ${tableName} ( ${selectParamSql.sql} )")

      logger.info(s"Populating rollup table ${tableName} for ${copyInfo} / ${rollupInfo} using sql: ${insertParamSql}")
      executeParamSqlUpdate(pgu.conn, insertParamSql)
    }
  }

  private def createIndexes(tableName: String, rollupInfo: RollupInfo, rollupReps: Seq[SqlColumnRep[SoQLType, SoQLValue] with Indexable[SoQLType]]) {
    time("create-indexes", "dataset_id" -> copyInfo.datasetInfo.systemId.underlying, "rollupName" -> rollupInfo.name.underlying) {
      using(pgu.conn.createStatement()) { stmt =>
        for {
          rep <- rollupReps
          createIndexSql <- rep.createIndex(tableName, tablespaceSql)
        } {
          // Currently we aren't using any SqlErrorHandlers here, because as of this
          // time none of the existing ones are appropriate.
          logger.trace(s"Creating index on ${tableName} for ${copyInfo} / ${rollupInfo} using sql: ${createIndexSql}")
          stmt.execute(createIndexSql)
        }
      }
    }
  }

  private def executeParamSqlUpdate(conn: Connection, pSql: ParametricSql): Int = {
    try {
      using(conn.prepareStatement(pSql.sql)) { stmt =>
        val stmt = conn.prepareStatement(pSql.sql)
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

  private def analysisToSoQLType(analysis: SoQLAnalysis[ColumnName, SoQLAnalysisType]): SoQLAnalysis[UserColumnId, SoQLType] = {
    val baos = new ByteArrayOutputStream
    SoQLAnalyzerHelper.serializer(baos, analysis.mapColumnIds(name => new UserColumnId(name.name)))
    SoQLAnalyzerHelper.deserializer(new ByteArrayInputStream(baos.toByteArray))
  }
}

object RollupManager {
  def rollupTableName(rollupInfo: RollupInfo, dataVersion: Long): String = {
    val sha1 = MessageDigest.getInstance("SHA-1")
    // we have a 63 char limit on table names, so just taking a prefix.  It only has to be
    // unique within a single dataset copy.
    val nameHash = sha1.digest(rollupInfo.name.underlying.getBytes("UTF-8")).take(8).map("%02X" format _).mkString.toLowerCase

    rollupInfo.copyInfo.dataTableName + "_r_" + dataVersion + "_" + nameHash
  }

  def makeSecondaryRollupInfo(rollupInfo: RollupInfo): SecondaryRollupInfo =
     SecondaryRollupInfo(rollupInfo.name.underlying, rollupInfo.soql)
}

