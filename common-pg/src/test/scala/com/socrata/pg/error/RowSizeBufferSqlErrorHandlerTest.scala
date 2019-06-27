package com.socrata.pg.error

import java.sql.{Connection, DriverManager, Statement}

import scala.util.{Success, Try}

import com.rojoma.simplearm.util._
import com.socrata.pg.store.SecondarySchemaLoader
import org.postgresql.util.PSQLException
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class RowSizeBufferSqlErrorHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll {

  val dbName = "secondary_sql_error_test"

  override def beforeAll(): Unit = {
    createDatabase(dbName)
  }

  test("row size buffer sql error continue") {
    handleRowSizeBufferishError("repeat('hello ', 100000)", Success(Some("\"i2\"")))
  }

  test("row size buffer sql error variation continue") {
    handleRowSizeBufferishError((60000 to 63000).mkString("'", ",", "'"), Success(None))
  }

  private def handleRowSizeBufferishError(largeContent: String, errorOnIndex: Try[Option[String]]): Unit = {
    withConnection(dbName) { conn =>
      using(conn.createStatement()) { stmt: Statement =>
        conn.setAutoCommit(false)
        stmt.execute("drop table if exists t1")
        stmt.execute("create table t1 (c1 text, c2 text)")
        stmt.execute("insert into t1 values('1', 'one')")
        stmt.execute(s"insert into t1 values('too long', $largeContent)")
        conn.commit()
        stmt.execute("create index i1 on t1 (c1)")
        val thrown = intercept[PSQLException] {
          stmt.execute("create index i2 on t1 (c2)")
        }
        RowSizeBufferSqlErrorHandler.isIndexRowSizeError(thrown) should be (errorOnIndex)
        intercept[PSQLException] {
          // cannot run another statement without explicit rollback or guard
          stmt.execute("insert into t1 values('2', 'two')")
        }
        conn.rollback()
        stmt.execute("insert into t1 values('2', 'two')")
        RowSizeBufferSqlErrorContinue.guard(conn) {
          stmt.execute("create index i2 on t1 (c2)")
        }
        // can run another statement after row size buffer error
        stmt.execute("insert into t1 values('3', 'can insert after row size buffer error')")
        conn.commit()
        val resultSet = stmt.executeQuery("select c2 from t1 where c1 in ('2', '3') order by t1")
        resultSet.next() should be (true)
        resultSet.getString(1) should be ("two")
        resultSet.next() should be (true)
        resultSet.getString(1) should be ("can insert after row size buffer error")
        conn.commit()
      }
    }
  }

  test("row is too big") {
    val range = Seq.range(1, 1000)
    val createColumns = range.map(n => s"c$n text").mkString(",")
    val allColumns = range.map(n => s"coalesce(c$n)").mkString("to_tsvector('english', ", " || ' ' || ", ")")
    val fullTextIndex = s"CREATE INDEX idx_search_t2 on t2 USING GIN ($allColumns)"
    withConnection(dbName) { conn =>
      using(conn.createStatement()) { stmt: Statement =>
        conn.setAutoCommit(false)
        stmt.execute(s"create table t2 ($createColumns)")
        val thrown = intercept[PSQLException] {
          stmt.execute(fullTextIndex)
        }
        thrown.getSQLState should be ("54001")
        conn.rollback()
        stmt.execute(s"create table t2 ($createColumns)")
        SecondarySchemaLoader.fullTextIndexCreateSqlErrorHandler.guard(conn) {
          stmt.execute(fullTextIndex)
        }
        conn.commit()
      }
    }
  }

  private def createDatabase(dbName: String): Unit = {
    try {
      Class.forName("org.postgresql.Driver").newInstance()
    } catch {
      case ex: ClassNotFoundException => throw ex
    }
    using(DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "blist", "blist")) { conn =>
      conn.setAutoCommit(true)
      val sql = s"drop database if exists $dbName; create database $dbName;"
      using(conn.createStatement()) { stmt =>
        stmt.execute(sql)
      }
    }
  }

  private def withConnection[T](dbName: String)(f: (Connection) => T): T = {
    using(DriverManager.getConnection(s"jdbc:postgresql://localhost:5432/$dbName", "blist", "blist")) { conn =>
      f(conn)
    }
  }
}
