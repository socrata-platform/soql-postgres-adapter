package com.socrata.pg.store

import liquibase.Liquibase
import liquibase.database.jvm.JdbcConnection
import liquibase.logging.LogFactory
import liquibase.resource.ClassLoaderResourceAccessor

import java.sql.Connection

import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation
import com.socrata.datacoordinator.truth.migration.Migration.MigrationOperation._

/**
 * Interface with the Liquibase library to perform schema migrations on a given database with a given set of changes.
 */
object Migration {

  /**
   * Performs a Liquibase schema migration operation on a given database.
   */
  def migrateDb(conn: Connection,
                operation: MigrationOperation = MigrationOperation.Migrate,
                changeLogPath: String = MigrationScriptPath): Unit = {

    LogFactory.getLogger().setLogLevel("warning")
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor, new JdbcConnection(conn))
    val database = conn.getCatalog

    operation match {
      case Migrate => liquibase.update(database)
      case Undo => liquibase.rollback(1, database)
      case Redo => { liquibase.rollback(1, database); liquibase.update(database) }
    }
  }

  private val MigrationScriptPath = "com/socrata/pg/store/schema/migrate.xml"
}
