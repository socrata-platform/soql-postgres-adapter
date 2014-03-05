package com.socrata.pg.store

import java.util.NoSuchElementException
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation

/**
 * This object takes Liquibase operations and performs according migrations to the pg-secondary schemas.
 */
object MigrateSchema extends App {
  /**
   * Performs a Liquibase schema migration.
   * @param args(0) Migration operation to perform.
   *        args(1) Database tree (located in config) to migrate.
   * */
  override def main(args: Array[String]) {
    // Verify that two arguments were passed
    if (args.length != 2)
      throw new IllegalArgumentException(
        s"Incorrect number of arguments - expected 2 but received ${args.length}")

    // Verify that the argument provided is actually a valid operation
    val operation = {
      try
        MigrationOperation.withName(args(0).toLowerCase.capitalize)
      catch {
        case ex: NoSuchElementException =>
          throw new IllegalArgumentException(
            s"No such migration operation: ${args(0)}. " +
              s"Available operations are [${MigrationOperation.values.mkString(", ")}]")
      }
    }
    val config = ConfigFactory.load.getConfig("com.socrata.pg.store")
    PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("log4j")))

    SchemaMigrator(args(1), operation, config)
  }
}
