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
   *        args(1) Optional dotted path reference the database config tree to migrate
   * */
  override def main(args: Array[String]) {
    // Verify that two arguments were passed
    if (args.length < 1 || args.length > 2)
      throw new IllegalArgumentException(
        s"Usage: com.socrata.pg.store.MigrateSchema ${MigrationOperation.values.mkString("|")} [<dotted path reference to database config in config>]")

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
    val config = ConfigFactory.load
    val dbConfigPath = args.length match {
      case 1 => "com.socrata.pg.store.database"
      case 2 => args(1)
    }

    PropertyConfigurator.configure(Propertizer("log4j", config.getConfig("com.socrata.pg.store.log4j")))

    SchemaMigrator(dbConfigPath, operation, config)
  }
}
