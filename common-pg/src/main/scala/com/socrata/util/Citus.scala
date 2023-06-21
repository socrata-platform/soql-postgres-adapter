package com.socrata.util

import com.rojoma.simplearm.v2.using
import com.socrata.datacoordinator.id.RollupName
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.util.Citus.Constant.IS_CITUS
import liquibase.database.Database
import liquibase.exception.CustomPreconditionFailedException
import liquibase.precondition.CustomPrecondition
import org.slf4j.Logger

import java.sql.{Connection, ResultSet, SQLException}

object Citus {

  val log: Logger = org.slf4j.LoggerFactory.getLogger(Citus.getClass)

  object Constant {
    val IS_CITUS = "IS_CITUS"
  }

  object MaybeDistribute {

    def sql(conn: Connection, resourceNameOptional: Option[String], tableName: String): Option[String] =
      resourceNameOptional.flatMap { resourceName =>
        sql(conn, new ResourceName(resourceName), TableName(tableName))
      }


    def sql(conn: Connection, resourceName: ResourceName, tableName: TableName): Option[String] =
      if (shouldUseCitusDistribution) {
        generate(conn, resourceName, tableName) match {
          case Right(x@Some(_)) => x
          case Right(None) =>
            log.info("No distribution key configured for dataset '%s'", resourceName.name)
            None
          case Left(e) =>
            log.error("Error while determining citus distribution key for dataset '%s'".format(resourceName.name), e)
            None
        }
      } else {
        None
      }


    def shouldUseCitusDistribution: Boolean = isCitus


    def isCitus: Boolean = sys.env.get(IS_CITUS).exists { raw => raw.toBoolean }

    private def generate(conn: Connection, resourceName: ResourceName, tableName: TableName): Either[SQLException, Option[String]] =
      getDistributionColumnOfDataset(conn, resourceName) match {
        case Right(Some(columnName)) => Right(Some("select create_distributed_table('%s', '%s');".format(tableName, columnName)))
        case other => other
      }

    private def getDistributionColumnOfDataset(conn: Connection, resourceName: ResourceName): Either[SQLException, Option[String]] =
      using(conn.prepareStatement("select column_name from citus_dataset_distribution_map where resource_name=?")) { stmt =>
        stmt.setString(1, resourceName.name)
        extractCitusDistributionColumnName(stmt.executeQuery())
      }

    private def extractCitusDistributionColumnName(rs: ResultSet): Either[SQLException, Option[String]] =
      try {
        if (rs.next()) {
          Right(Some(rs.getString("column_name")))
        } else {
          Right(None)
        }
      } catch {
        case e: SQLException => Left(e)
      }

    def sql(conn: Connection, resourceNameOptional: Option[String], tableName: String, rollupName: RollupName): Option[String] =
      resourceNameOptional.flatMap { resourceName =>
        sql(conn, new ResourceName(resourceName), TableName(tableName), rollupName)
      }

    def sql(conn: Connection, resourceName: ResourceName, tableName: TableName, rollupName: RollupName): Option[String] =
      if (shouldUseCitusDistribution) {
        generate(conn, resourceName, tableName, rollupName) match {
          case Right(x@Some(_)) => x
          case Right(None) =>
            log.info("No distribution key configured for dataset '%s', rollup '%s'".format(resourceName.name, rollupName.underlying))
            None
          case Left(e) =>
            log.error("Error while determining citus distribution key for dataset '%s', rollup '%s'".format(resourceName.name, rollupName.underlying), e)
            None
        }
      } else {
        None
      }

    private def generate(conn: Connection, resourceName: ResourceName, tableName: TableName, rollupName: RollupName): Either[SQLException, Option[String]] =
      getDistributionColumnOfRollup(conn, resourceName, rollupName) match {
        case Right(Some(columnName)) => Right(Some("select create_distributed_table('%s', '%s');".format(tableName, columnName)))
        case other => other
      }

    private def getDistributionColumnOfRollup(conn: Connection, resourceName: ResourceName, rollupName: RollupName): Either[SQLException, Option[String]] =
      using(conn.prepareStatement("select column_name from citus_rollup_distribution_map where resource_name=? and rollup_name=?")) { stmt =>
        stmt.setString(1, resourceName.name)
        stmt.setString(2, rollupName.underlying)
        extractCitusDistributionColumnName(stmt.executeQuery())
      }

    class CitusPrecondition extends CustomPrecondition {
      override def check(database: Database): Unit =
        if (!isCitus) { throw new CustomPreconditionFailedException("Not a citus node") }
    }
  }
}
