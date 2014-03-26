package com.socrata.pg.error

import com.rojoma.simplearm.util._
import java.sql.{Statement, DriverManager, Connection}
import org.postgresql.util.PSQLException
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuite}


class RowSizeBufferSqlErrorHandlerTest extends FunSuite with Matchers with BeforeAndAfterAll {

  val dbName = "secondary_sql_error_test"

  override def beforeAll() {
    createDatabase(dbName)
  }

  test("row size buffer sql error continue") {
    withConnection(dbName) { conn =>
      using(conn.createStatement()) { stmt: Statement =>
        conn.setAutoCommit(false)
        stmt.execute("create table t1 (c1 text, c2 text)")
        stmt.execute("insert into t1 values('1', 'one')")
        // this lengthy content causes subsequent create index to fail on column c2
        stmt.execute("insert into t1 values('too long', repeat('hello ', 100000))")
        conn.commit()
        stmt.execute("create index i1 on t1 (c1)")
        val thrown = intercept[PSQLException] {
          stmt.execute("create index i2 on t1 (c2)")
        }
        RowSizeBufferSqlErrorHandler.isIndexRowSizeError(thrown) should be (Some("\"i2\""))
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

  private def createDatabase(dbName: String) {
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
