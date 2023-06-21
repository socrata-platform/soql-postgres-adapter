package com.socrata.util

import com.rojoma.simplearm.v2.using
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.util.Citus.Constant.IS_CITUS
import org.slf4j.Logger

import java.sql.{Connection, ResultSet, SQLException}

object Citus {

  val log: Logger = org.slf4j.LoggerFactory.getLogger(Citus.getClass)

  object Constant {
    val IS_CITUS = "IS_CITUS"
  }

  object MaybeDistribute {

    def sql(conn: Connection, resourceNameOptional: Option[String], tableName: String): Option[String] = {
      if (shouldUseCitusDistribution) {
        resourceNameOptional.flatMap { resourceName =>
          generate(conn, new ResourceName(resourceName), TableName(tableName)) match {
            case Left(x@Some(_)) => x
            case Left(None) => log.info("No distribution key configured for %s", resourceName)
              None
            case Right(e) => log.error("Error while determining citus distribution key for dataset %s".format(resourceName), e)
              None
          }
        }
      } else {
        None
      }
    }

    def shouldUseCitusDistribution: Boolean = {
      sys.env.get(IS_CITUS).exists { raw => raw.toBoolean }
    }

    private def generate(conn: Connection, resourceName: ResourceName, tableName: TableName): Either[Option[String], SQLException] = {
      getDistributionColumnOfDataset(conn, resourceName) match {
        case Left(Some(columnName)) => Left(Some("select create_distributed_table('%s', '%s');".format(tableName, columnName)))
        case x@_ => x
      }
    }

    private def getDistributionColumnOfDataset(conn: Connection, resourceName: ResourceName): Either[Option[String], SQLException] = {
      using(conn.prepareStatement("select column_name from citus_distribution_map where resource_name=?")) { stmt =>
        stmt.setString(1, resourceName.name)
        extractCitusDistributionColumnName(stmt.executeQuery())
      }
    }

    private def extractCitusDistributionColumnName(rs: ResultSet): Either[Option[String], SQLException] = {
      try {
        if (rs.next()) {
          Left(Some(rs.getString("column_name")))
        } else {
          Left(None)
        }
      } catch {
        case e: SQLException => Right(e)
      }
    }
  }
}
