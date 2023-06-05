package com.socrata.pg.analyzer2

import java.sql.ResultSet
import com.socrata.soql.analyzer2._
import com.socrata.prettyprint.prelude._

case class SubcolInfo[MT <: MetaTypes](index: Int, sqlType: String, soqlType: types.ColumnType[MT])

trait Rep[MT <: MetaTypes] extends ExpressionUniverse[MT] {
  def typ: CT
  def physicalColumnRef(col: PhysicalColumn)(implicit gensymProvider: GensymProvider): ExprSql[MT]
  def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean)(implicit gensymProvider: GensymProvider): ExprSql[MT]
  def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider): ExprSql[MT]
  def literal(value: LiteralValue)(implicit gensymProvider: GensymProvider): ExprSql[MT] // type of literal will be appropriate for this rep
  def expandedColumnCount: Int
  def expandedDatabaseColumns(name: ColumnLabel): Seq[Doc[Nothing]]
  def compressedDatabaseColumn(name: ColumnLabel): Doc[Nothing]
  def isProvenanced: Boolean = false
  def provenanceOf(value: LiteralValue): Set[CanonicalName]

  // throws an exception if "field" is not a subcolumn of this type
  def subcolInfo(field: String): SubcolInfo[MT]

  // This lets us produce a different representation in the top-level
  // selection if desired (e.g., for geometries we want to convert
  // them to WKB).  For most things this is false and wrapTopLevel is
  // just the identity function.
  def hasTopLevelWrapper: Boolean = false
  def wrapTopLevel(raw: ExprSql[MT]): ExprSql[MT] = {
    assert(raw.typ == typ)
    raw
  }

  def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV)
}
object Rep {
  trait Provider[MT <: MetaTypes] extends SqlizerUniverse[MT] {
    protected implicit def implicitProvider: Provider[MT] = this
    def apply(typ: CT): Rep

    val namespace: SqlNamespaces

    def mkStringLiteral(name: String): Doc

    protected abstract class SingleColumnRep(val typ: CT, val sqlType: Doc) extends Rep {
      def physicalColumnRef(col: PhysicalColumn)(implicit gensymProvider: GensymProvider) =
        ExprSql(Seq(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)), col)

      def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean)(implicit gensymProvider: GensymProvider) =
        if(isExpanded) {
          ExprSql(Seq(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)), col)
        } else {
          ExprSql(namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }

      def expandedColumnCount = 1

      def expandedDatabaseColumns(name: ColumnLabel) = Seq(compressedDatabaseColumn(name))

      def compressedDatabaseColumn(name: ColumnLabel) = namespace.columnBase(name)

      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql(d"null ::" +#+ sqlType, e)

      def subcolInfo(field: String) = throw new Exception(s"$typ has no sub-columns")

      def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = { (rs, dbCol) =>
        (1, doExtractFrom(rs, dbCol))
      }

      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV

      def provenanceOf(e: LiteralValue) = Set.empty
    }

    protected abstract class CompoundColumnRep(val typ: CT) extends Rep {
      def physicalColumnRef(col: PhysicalColumn)(implicit gensymProvider: GensymProvider) =
        genericColumnRef(col, true)

      def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean)(implicit gensymProvider: GensymProvider) =
        genericColumnRef(col, isExpanded)

      private def genericColumnRef(col: Column, isExpanded: Boolean)(implicit gensymProvider: GensymProvider): ExprSql = {
        val dbTable = namespace.tableLabel(col.table)
        if(isExpanded) {
          ExprSql(expandedDatabaseColumns(col.column).map { cn => dbTable ++ d"." ++ cn }, col)
        } else {
          ExprSql(dbTable ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }
      }

      def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = {
        if(isExpanded) { (rs, dbCol) =>
          (expandedColumnCount, doExtractExpanded(rs, dbCol))
        } else { (rs, dbCol) =>
          (1, doExtractCompressed(rs, dbCol))
        }
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV

      def provenanceOf(e: LiteralValue) = Set.empty
    }

    protected abstract class ProvenancedRep(val typ: CT, primarySqlTyp: Doc) extends Rep {
      // We'll be representing provenanced types (SoQLID and
      // SoQLVersion) a little weirdly because we want to track the
      // values' provenance
      // So:
      //   * physical tables contain only a primarySqlTyp (probaby "bigint")
      //   * virtual tables contain both the primarySqlTyp and the canonical name of the table it came from
      // The provenance comes first so that you can't use comparisons
      // with a table under your control to find out information about
      // intervals between IDs in tables you don't control.

      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql.Expanded[MT](Seq(d"null :: text", d"null ::" +#+ primarySqlTyp), e)

      def expandedColumnCount = 2

      def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_provenance", base)
      }

      def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      def physicalColumnRef(col: PhysicalColumn)(implicit gensymProvider: GensymProvider) = {
        // The "::text" is required so that the provenance is not a
        // literal by SQL's standards.  Otherwise this will have
        // trouble if you order or group by :id
        val dsTable = namespace.tableLabel(col.table)
        ExprSql.Expanded[MT](Seq(mkStringLiteral(col.tableCanonicalName.name) +#+ d":: text", dsTable ++ d"." ++ compressedDatabaseColumn(col.column)), col)
      }

      def virtualColumnRef(col: VirtualColumn, isExpanded: Boolean)(implicit gensymProvider: GensymProvider) = {
        val dsTable = namespace.tableLabel(col.table)
        if(isExpanded) {
          ExprSql(expandedDatabaseColumns(col.column).map { cn => dsTable ++ d"." ++ cn }, col)
        } else {
          ExprSql(dsTable ++ d"." ++ compressedDatabaseColumn(col.column), col)
        }
      }

      override def isProvenanced = true

      def subcolInfo(field: String) = throw new Exception(s"$typ has no sub-columns")

      def extractFrom(isExpanded: Boolean): (ResultSet, Int) => (Int, CV) = {
        if(isExpanded) { (rs, dbCol) =>
          (expandedColumnCount, doExtractExpanded(rs, dbCol))
        } else { (rs, dbCol) =>
          (1, doExtractCompressed(rs, dbCol))
        }
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV
    }
  }
}

