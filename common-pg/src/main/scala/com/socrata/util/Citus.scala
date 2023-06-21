package com.socrata.util

import com.rojoma.simplearm.v2.using
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.util.Citus.Constant.IS_CITUS
import org.slf4j.Logger

import java.sql.{Connection, ResultSet, SQLException}

object Citus {

  val log: Logger = org.slf4j.LoggerFactory.getLogger(Citus.getClass)

  def shouldUseCitusDistribution: Boolean = {
    sys.env.get(IS_CITUS).map{raw=>}.getOrElse(false)
  }

  private def distributeTable(conn: Connection, resourceName: ResourceName, tableName: TableName): Option[String] = {
    getDistributionColumnOfDataset(conn, resourceName).map { column =>
      "select create_distributed_table('%s', '%s');".format(tableName, column)
    }
  }

  private def getDistributionColumnOfDataset(conn: Connection, resourceName: ResourceName): Option[String] = {
    using(conn.prepareStatement("select column_name from citus_distribution_map where resource_name=?")) { stmt =>
      stmt.setString(1, resourceName.name)
      extractCitusDistributionColumnName(stmt.executeQuery()) match {
        case Left(columnNameOptional) => columnNameOptional
        case Right(e) => {
          log.error("Error determining citus distribution key for dataset %s".format(resourceName.name),e)
          None
        }
      }
    }
  }

  private def extractCitusDistributionColumnName(rs: ResultSet): Either[Option[String], SQLException] = {
    if (rs.next()) {
      try {
        Left(Some(rs.getString("column_name")))
      } catch {
        case e: SQLException => Right(e)
      }
    } else {
      Left(None)
    }
  }

  object Constant {
    val IS_CITUS = "IS_CITUS"
  }

  object MaybeDistribute {
    def sql(conn: Connection, resourceName: Option[String], tableName: String): Option[String] = {
      if (shouldUseCitusDistribution) {
        resourceName.flatMap { resource => Citus.distributeTable(conn, new ResourceName(resource), TableName(tableName)) }
      } else {
        None
      }
    }
  }
}
