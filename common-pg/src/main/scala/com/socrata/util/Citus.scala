package com.socrata.util

import com.rojoma.simplearm.v2.using
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.util.Citus.Constant.IS_CITUS

import java.sql.{Connection, ResultSet}

object Citus {

  private def distributeTable(conn: Connection, resourceName: ResourceName, tableName: TableName): String = {
    "select create_distributed_table('%s', '%s');".format(tableName, getDistributionColumnOfDataset(conn, resourceName))
  }

  private def getDistributionColumnOfDataset(conn: Connection, resourceName: ResourceName): Option[String] = {
    using(conn.prepareStatement("select column_name from citus_distribution_map where resource_name=?")) { stmt =>
      stmt.setString(1, resourceName.name)
      extractCitusDistributionColumnName(stmt.executeQuery())
    }
  }

  private def extractCitusDistributionColumnName(rs: ResultSet): Option[String] = {
    if (rs.next()) {
      Some(rs.getString("column_name"))
    } else {
      None
    }
  }

  object Constant {
    val IS_CITUS = "IS_CITUS"
  }

  def shouldUseCitusDistribution: Boolean = {
    sys.env.contains(IS_CITUS)
  }

  object MaybeDistribute {
    def sql(conn: Connection, resourceName: Option[String], tableName: String): Option[String] = {
      if (shouldUseCitusDistribution) {
        resourceName.map { resource => Citus.distributeTable(conn, new ResourceName(resource), TableName(tableName)) }
      } else {
        None
      }
    }
  }
}
