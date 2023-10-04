package com.socrata.pg.analyzer2

import java.sql.ResultSet

import com.rojoma.json.v3.ast.JString

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance

class TestRepProvider(
  override val namespace: SqlNamespaces[TestHelper.TestMT],
  override val toProvenance: types.ToProvenance[TestHelper.TestMT],
  override val isRollup: types.DatabaseTableName[TestHelper.TestMT] => Boolean
) extends Rep.Provider[TestHelper.TestMT] {
  type TestMT = TestHelper.TestMT

  override def mkStringLiteral(s: String) =
    Doc(JString(s).toString)

  def apply(typ: TestType): Rep = reps(typ)

  val reps = Map[TestType, Rep](
    TestID -> new ProvenancedRep(TestID, d"bigint") {
      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[TestID]
        Set(rawId.provenance)
      }

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[TestID]

        val provLit = rawId.provenance match {
          case None => d"null :: text"
          case Some(Provenance(s)) => mkTextLiteral(s)
        }
        val numLit = Doc(rawId.value) +#+ d":: bigint"

        ExprSql.Expanded[TestMT](Seq(provLit, numLit), e)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
        )
      }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestText -> new SingleColumnRep(TestText, d"text") {
      override def literal(e: LiteralValue) = {
        val TestText(s) = e.value
        ExprSql(mkTextLiteral(s), e)
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestNumber -> new SingleColumnRep(TestNumber, d"numeric") {
      override def literal(e: LiteralValue) = {
        val TestNumber(n) = e.value
        ExprSql(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },
    TestBoolean -> new SingleColumnRep(TestBoolean, d"boolean") {
      override def literal(e: LiteralValue) = {
        val TestBoolean(b) = e.value
        ExprSql(if(b) d"true" else d"false", e)
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    },

    TestCompound -> new CompoundColumnRep(TestCompound) {
      override def nullLiteral(e: NullLiteral) =
        ExprSql.Expanded[TestMT](Seq(d"null :: text", d"null :: numeric"), e)

      override def expandedColumnCount = 2

      override def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_a", base ++ d"_b")
      }

      override def expandedDatabaseTypes = Seq(d"text", d"numeric")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(aName, bName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ aName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: numeric AS" +#+ bName,
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      override def literal(e: LiteralValue) = {
        val cmp@TestCompound(_, _) = e.value

        cmp match {
          case TestCompound(None, None) =>
            ExprSql.Expanded[TestMT](Seq(d"null :: text", d"null :: numeric"), e)
          case TestCompound(a, b) =>
            val aLit = a match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val bLit = b match {
              case Some(n) => Doc(n.toString) +#+ d" :: numeric"
              case None => d"null :: numeric"
            }

            ExprSql.Expanded[TestMT](Seq(aLit, bLit), e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "a" => SubcolInfo[TestMT](TestCompound, 0, "text", TestText, _.parenthesized +#+ d"->> 0")
          case "b" => SubcolInfo[TestMT](TestCompound, 1, "numeric", TestNumber, { e => (e.parenthesized +#+ d"->> 1").parenthesized +#+ d":: numeric" }) // ->> because it handles jsonb null => sql null
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        ???
      }

      override def indices(tableName: DatabaseTableName, label: ColumnLabel) = {
        ???
      }
    }
  )
}
