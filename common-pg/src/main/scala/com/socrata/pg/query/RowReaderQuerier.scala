package com.socrata.pg.query

import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.datacoordinator.truth.loader.sql.DataSqlizer
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.pg.soql.{ParametricSql, QualifiedUserColumnId}
import com.socrata.pg.store.PGSecondaryRowReader
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.stdlib.{Context => SoQLContext}

import scala.concurrent.duration.Duration

trait RowReaderQuerier[CT, CV] {
  this: PGSecondaryRowReader[CT, CV] => ()

  def query(context: SoQLContext,
            analyses: BinaryTree[SoQLAnalysis[UserColumnId, CT]],
            toSql: (BinaryTree[SoQLAnalysis[UserColumnId, CT]]) => ParametricSql,
            toRowCountSql: (BinaryTree[SoQLAnalysis[UserColumnId, CT]]) => ParametricSql,
            reqRowCount: Boolean,
            querySchema: OrderedMap[ColumnId, SqlColumnRep[CT, CV]],
            queryTimeout: Option[Duration],
            debug: Boolean):
            Managed[CloseableIterator[com.socrata.datacoordinator.Row[CV]] with RowCount] = {
    val sqlizerq = sqlizer.asInstanceOf[DataSqlizer[CT, CV] with DataSqlizerQuerier[CT, CV]]
    val resultIter = sqlizerq.query(connection, context, analyses, toSql, toRowCountSql, reqRowCount, querySchema, queryTimeout, debug)
    managed(resultIter)
  }

  def queryExplain(context: SoQLContext,
                   analyses: BinaryTree[SoQLAnalysis[UserColumnId, CT]],
                   toSql: (BinaryTree[SoQLAnalysis[UserColumnId, CT]]) => ParametricSql,
                   queryTimeout: Option[Duration],
                   debug: Boolean,
                   analyze: Boolean): ExplainInfo = {
    val sqlizerq = sqlizer.asInstanceOf[DataSqlizer[CT, CV] with DataSqlizerQuerier[CT, CV]]

    sqlizerq.explainQuery(connection, context, analyses, toSql, queryTimeout, analyze)
  }

  def getSqlReps(datatableName: String, systemToUserColumnMap: Map[ColumnId, UserColumnId]): Map[QualifiedUserColumnId, SqlColumnRep[CT, CV]] = {
    val sqlizerq = sqlizer.asInstanceOf[DataSqlizer[CT, CV] with DataSqlizerQuerier[CT, CV]]
    val userColumnIdRepMap = sqlizerq.repSchema.foldLeft(Map.empty[QualifiedUserColumnId, SqlColumnRep[CT, CV]]) {
      (map, colIdAndsqlRep) =>
        colIdAndsqlRep match {
          case (columnId, sqlRep) => map + (QualifiedUserColumnId(None, systemToUserColumnMap(columnId)) -> sqlRep) +
            (QualifiedUserColumnId(Some(datatableName), systemToUserColumnMap(columnId)) -> sqlRep)
        }
    }
    userColumnIdRepMap
  }
}
