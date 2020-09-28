package com.socrata.pg.server

import java.util.NoSuchElementException
import com.typesafe.config.ConfigFactory
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation

import com.socrata.pg.store.SchemaMigrator

/**
 * This object takes Liquibase operations and performs according migrations to the pg-secondary schemas.
 */
object MigrateSchema extends App {
  /**
   * Performs a Liquibase schema migration.
   * @param args(0) Migration operation to perform.
   *        args(1) Optional dotted path reference the database config tree to migrate
   * */
  // Verify that two arguments were passed
  if (args.length < 1 || args.length > 3) {
    throw new IllegalArgumentException(
      "Usage: com.socrata.pg.server.MigrateSchema " +
        MigrationOperation.values.mkString("|") +
        " [<dotted path reference to database config in config>]")
  }

  val dryRun = args.exists(_ == "--dry-run")

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

  val coreArgs = args.filter(!_.startsWith("--"))
  val configRoot = coreArgs.length match {
    case 1 =>
      // Run migration on each secondary instance from the perspective of secondary-watcher/pg store plugin.
      "com.socrata.pg.store"
    case 2 => coreArgs(1)
  }

  val dbConfigPath = s"$configRoot.database"

  PropertyConfigurator.configure(Propertizer("log4j", config.getConfig(s"$configRoot.log4j")))

  SchemaMigrator(dbConfigPath, operation, config, dryRun)
}
