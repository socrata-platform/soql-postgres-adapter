package com.socrata.pg.store

import java.io.OutputStreamWriter

import liquibase.Liquibase
import liquibase.lockservice.LockService
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
                changeLogPath: String = MigrationScriptPath,
                dryRun: Boolean = false): Unit = {

    val jdbc = new NonCommmittingJdbcConnenction(conn)
    LogFactory.getLogger().setLogLevel("warning")
    val liquibase = new Liquibase(changeLogPath, new ClassLoaderResourceAccessor, jdbc)
    val lockService = LockService.getInstance(liquibase.getDatabase)
    lockService.setChangeLogLockWaitTime(1000 * 3) // 3s where value should be < SQL lock_timeout (30s)
    val database = conn.getCatalog

    operation match {
      case Migrate =>
        if (dryRun) liquibase.update(database, new OutputStreamWriter(System.out))
        else liquibase.update(database)

      case Undo => liquibase.rollback(1, database)
      case Redo => { liquibase.rollback(1, database); liquibase.update(database) }
    }
    jdbc.realCommit()
  }

  private val MigrationScriptPath = "com/socrata/pg/store/schema/migrate.xml"
}
