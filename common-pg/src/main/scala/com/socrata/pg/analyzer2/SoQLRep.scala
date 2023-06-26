package com.socrata.pg.analyzer2

import java.sql.ResultSet

import com.rojoma.json.v3.ast.{JNull, JValue}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.util.OrJNull.implicits._
import com.vividsolutions.jts.geom.Geometry

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types._

abstract class SoQLRepProvider[MT <: MetaTypes with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = String})](
  cryptProviders: CryptProviderProvider,
  override val namespace: SqlNamespaces[MT],
  locationSubcolumns: Map[types.DatabaseTableName[MT], Map[types.DatabaseColumnName[MT], Seq[Option[types.DatabaseColumnName[MT]]]]],
  physicalTableFor: Map[AutoTableLabel, types.DatabaseTableName[MT]]
) extends Rep.Provider[MT] {
  def apply(typ: SoQLType) = reps(typ)

  class GeometryRep[T <: Geometry](t: SoQLType with SoQLGeometryLike[T], ctor: T => CV, name: String) extends SingleColumnRep(t, d"geometry") {
    private val open = d"st_${name}fromtext"

    def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
      val SoQLText(s) = e.value
      ExprSql(Seq(mkStringLiteral(s).funcall(open), Geo.defaultSRIDLiteral).funcall(d"st_setsrid"), e)
    }

    override def hasTopLevelWrapper = true
    override def wrapTopLevel(raw: ExprSql) = {
      assert(raw.typ == typ)
      ExprSql(raw.compressed.sql.funcall(d"st_asbinary"), raw.expr)
    }

    def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
      Option(rs.getBytes(dbCol)).flatMap { bytes =>
        t.WkbRep.unapply(bytes) // TODO: this just turns invalid values into null, we should probably be noisier than that
      }.map(ctor).getOrElse(SoQLNull)
    }
  }

  val reps = Map[SoQLType, Rep](
    SoQLID -> new ProvenancedRep(SoQLID, d"bigint") {
      def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLID]
        rawId.provenance.map(CanonicalName(_)).toSet
      }

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val rawId = e.value.asInstanceOf[SoQLID]
        val rawFormatted = SoQLID.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(s) => mkStringLiteral(s) +#+ d":: text"
          }
        val numLit =
          rawId.provenance.map(CanonicalName(_)).flatMap(cryptProviders) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLID.StringRep(cryptProvider)
              val SoQLID(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        ExprSql.Expanded[MT](Seq(provenanceLit, numLit), e)
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol))
        val valueRaw = rs.getLong(dbCol + 1)

        if(rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLID(valueRaw)
          result.provenance = provenance
          result
        }
      }

      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLID(v)
                result.provenance = Some(prov)
                result
              case Right((Left(JNull), v)) =>
                SoQLID(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },
    SoQLVersion -> new ProvenancedRep(SoQLVersion, d"bigint") {
      def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        rawId.provenance.map(CanonicalName(_)).toSet
      }

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        val rawFormatted = SoQLVersion.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(s) => mkStringLiteral(s) +#+ d":: text"
          }
        val numLit =
          rawId.provenance.map(CanonicalName(_)).flatMap(cryptProviders) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLVersion.StringRep(cryptProvider)
              val SoQLVersion(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        ExprSql.Expanded[MT](Seq(provenanceLit, numLit), e)
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol))
        val valueRaw = rs.getLong(dbCol + 1)

        if(rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLVersion(valueRaw)
          result.provenance = provenance
          result
        }
      }

      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLVersion(v)
                result.provenance = Some(prov)
                result
              case Right((Left(JNull), v)) =>
                SoQLVersion(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    // ATOMIC REPS

    SoQLText -> new SingleColumnRep(SoQLText, d"text") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLText(s) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(s), e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLText(t)
        }
      }
    },
    SoQLNumber -> new SingleColumnRep(SoQLNumber, d"numeric") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLNumber(n) = e.value
        ExprSql(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getBigDecimal(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLNumber(t)
        }
      }
    },
    SoQLBoolean -> new SingleColumnRep(SoQLBoolean, d"boolean") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLBoolean(b) = e.value
        ExprSql(if(b) d"true" else d"false", e)
      }
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        val v = rs.getBoolean(dbCol)
        if(rs.wasNull) {
          SoQLNull
        } else {
          SoQLBoolean(v)
        }
      }
    },
    SoQLFixedTimestamp -> new SingleColumnRep(SoQLFixedTimestamp, d"timestamp with time zone") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLFixedTimestamp(s) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(SoQLFixedTimestamp.StringRep(s)), e)
      }
      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.FixedTimestampRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
    },
    SoQLFloatingTimestamp -> new SingleColumnRep(SoQLFloatingTimestamp, d"timestamp without time zone") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLFloatingTimestamp(s) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(SoQLFloatingTimestamp.StringRep(s)), e)
      }
      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.FloatingTimestampRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
    },
    SoQLDate -> new SingleColumnRep(SoQLDate, d"date") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLDate(s) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(SoQLDate.StringRep(s)), e)
      }
      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.DateRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
    },
    SoQLTime -> new SingleColumnRep(SoQLTime, d"time without time zone") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLTime(s) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(SoQLTime.StringRep(s)), e)
      }
      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.TimeRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
    },
    SoQLJson -> new SingleColumnRep(SoQLJson, d"jsonb") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val SoQLJson(j) = e.value
        ExprSql(sqlType +#+ mkStringLiteral(CompactJsonWriter.toString(j)), e)
      }
      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.JsonRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
    },

    SoQLDocument -> new SingleColumnRep(SoQLDocument, d"jsonb") {
      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = ??? // no such thing as a doc liteal
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case Some(s) =>
            JsonUtil.parseJson[SoQLDocument](s) match {
              case Right(doc) => doc
              case Left(err) => throw new Exception("Unexpected document json from database: " + err.english)
            }
          case None =>
            SoQLNull
        }
      }
    },

    SoQLPoint -> new GeometryRep(SoQLPoint, SoQLPoint(_), "point"),
    SoQLMultiPoint -> new GeometryRep(SoQLMultiPoint, SoQLMultiPoint(_), "mpoint"),
    SoQLLine -> new GeometryRep(SoQLLine, SoQLLine(_), "line"),
    SoQLMultiLine -> new GeometryRep(SoQLMultiLine, SoQLMultiLine(_), "mline"),
    SoQLPolygon -> new GeometryRep(SoQLPolygon, SoQLPolygon(_), "polygon"),
    SoQLMultiPolygon -> new GeometryRep(SoQLMultiPolygon, SoQLMultiPolygon(_), "mpoly"),

    // COMPOUND REPS

    SoQLPhone -> new CompoundColumnRep(SoQLPhone) {
      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql.Expanded[MT](Seq(d"null :: text", d"null :: text"), e)

      def expandedColumnCount = 2

      def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_number", base ++ d"_type")
      }

      def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val ph@SoQLPhone(_, _) = e.value

        ph match {
          case SoQLPhone(None, None) =>
            ExprSql(d"null :: jsonb", e)
          case SoQLPhone(phNum, phTyp) =>
            val numberLit = phNum match {
              case Some(n) => mkStringLiteral(n)
              case None => d"null :: text"
            }
            val typLit = phTyp match {
              case Some(t) => mkStringLiteral(t)
              case None => d"null :: text"
            }

            ExprSql(d"jsonb_build_array(" ++ numberLit ++ d"," ++ typLit ++ d")", e)
        }
      }

      def subcolInfo(field: String) =
        field match {
          case "phone_number" => SubcolInfo[MT](SoQLPhone, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "phone_type" => SubcolInfo[MT](SoQLPhone, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.PhoneRep("")
      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Either[JNull, String])](v) match {
              case Right((Left(_), Left(_))) =>
                SoQLNull
              case Right((Left(_), Right(s))) =>
                SoQLPhone(None, Some(s))
              case Right((Right(s), Left(_))) =>
                SoQLPhone(Some(s), None)
              case Right((Right(s1), Right(s2))) =>
                SoQLPhone(Some(s1), Some(s2))
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    SoQLLocation -> new CompoundColumnRep(SoQLLocation) {
      override def physicalColumnRef(col: PhysicalColumn)(implicit gensymProvider: GensymProvider) = {
        val colInfo: Seq[Option[DatabaseColumnName]] = locationSubcolumns(physicalTableFor(col.table))(col.column)

        ExprSql(
          (namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)) +:
            colInfo.map {
              case Some(DatabaseColumnName(dcn)) =>
                (namespace.tableLabel(col.table) ++ d"." ++ Doc(dcn))
              case None =>
                d"null :: text"
            },
          col)
      }

      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql.Expanded[MT](Seq(d"null :: point", d"null :: text", d"null :: text", d"null :: text", d"null :: text"), e)

      def expandedColumnCount = 5

      def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base, base ++ d"_address", base ++ d"_city", base ++ d"_state", base ++ d"_zip")
      }

      def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val loc@SoQLLocation(_, _, _) = e.value
        ??? // No such thing as a location literal
      }

      def subcolInfo(field: String) =
        field match {
          case "point" => SubcolInfo[MT](SoQLLocation, 0, "geometry", SoQLPoint, { e => Seq(e.parenthesized +#+ d"-> 0").funcall(d"st_geomfromgeojson") })
          case "address" => SubcolInfo[MT](SoQLLocation, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
          case "city" => SubcolInfo[MT](SoQLLocation, 2, "text", SoQLText, _.parenthesized +#+ d"->> 2")
          case "state" => SubcolInfo[MT](SoQLLocation, 3, "text", SoQLText, _.parenthesized +#+ d"->> 3")
          case "zip" => SubcolInfo[MT](SoQLLocation, 4, "text", SoQLText, _.parenthesized +#+ d"->> 4")
        }

      override def hasTopLevelWrapper = true
      override def wrapTopLevel(raw: ExprSql) = {
        assert(raw.typ == SoQLLocation)
        raw match {
          case compressed: ExprSql.Compressed[MT] =>
            compressed
          case expanded: ExprSql.Expanded[MT] =>
            val sqls = expanded.sqls
            assert(sqls.length == expandedColumnCount)
            ExprSql.Expanded(Seq(sqls.head).funcall(d"st_asbinary") +: sqls.tail, expanded.expr)(SoQLRepProvider.this, namespace)
        }
      }

      implicit class ToBigDecimalAugmentation(val x: Double) {
        def toBigDecimal = java.math.BigDecimal.valueOf(x)
      }

      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val point = Option(rs.getBytes(dbCol)).flatMap { bytes =>
          SoQLPoint.WkbRep.unapply(bytes)
        }
        val address = Option(rs.getString(dbCol+1))
        val city = Option(rs.getString(dbCol+2))
        val state = Option(rs.getString(dbCol+3))
        val zip = Option(rs.getString(dbCol+4))

        if(address.isEmpty && city.isEmpty && state.isEmpty && zip.isEmpty) {
          if(point.isEmpty) {
            SoQLNull
          } else {
            SoQLLocation(point.map(_.getY.toBigDecimal), point.map(_.getX.toBigDecimal), None)
          }
        } else {
          SoQLLocation(
            point.map(_.getY.toBigDecimal), point.map(_.getX.toBigDecimal),
            Some(
              CompactJsonWriter.toString(json"""{address: ${address.orJNull}, city: ${city.orJNull}, state: ${state.orJNull}, zip: ${zip.orJNull}}""")
            )
          )
        }
      }
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, JValue], Either[JNull, String], Either[JNull, String], Either[JNull, String], Either[JNull, String])](v) match {
              case Right((Left(_), Left(_), Left(_), Left(_), Left(_))) =>
                SoQLNull
              case Right((pt, address, city, state, zip)) =>
                val point = pt.toOption.flatMap { ptJson => SoQLPoint.JsonRep.unapply(ptJson.toString) }
                SoQLLocation(
                  point.map(_.getY.toBigDecimal), point.map(_.getX.toBigDecimal),
                  Some(
                    CompactJsonWriter.toString(json"""{address: ${address.toOption.orJNull}, city: ${city.toOption.orJNull}, state: ${state.toOption.orJNull}, zip: ${zip.toOption.orJNull}}""")
                  )
                )
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    SoQLUrl -> new CompoundColumnRep(SoQLUrl) {
      def nullLiteral(e: NullLiteral)(implicit gensymProvider: GensymProvider) =
        ExprSql.Expanded[MT](Seq(d"null :: text", d"null :: text"), e)

      def expandedColumnCount = 2

      def expandedDatabaseColumns(name: ColumnLabel) = {
        val base = namespace.columnBase(name)
        Seq(base ++ d"_url", base ++ d"_description")
      }

      def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnBase(name)

      def literal(e: LiteralValue)(implicit gensymProvider: GensymProvider) = {
        val url@SoQLUrl(_, _) = e.value

        url match {
          case SoQLUrl(None, None) =>
            ExprSql.Expanded[MT](Seq(d"null :: text", d"null :: text"), e)
          case SoQLUrl(urlUrl, urlDesc) =>
            val urlLit = urlUrl match {
              case Some(n) => mkStringLiteral(n)
              case None => d"null :: text"
            }
            val descLit = urlDesc match {
              case Some(t) => mkStringLiteral(t)
              case None => d"null :: text"
            }

            ExprSql.Expanded[MT](Seq(urlLit, descLit), e)
        }
      }

      def subcolInfo(field: String) =
        field match {
          case "url" => SubcolInfo[MT](SoQLUrl, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "description" => SubcolInfo[MT](SoQLUrl, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.UrlRep("")
      protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Either[JNull, String])](v) match {
              case Right((Left(_), Left(_))) =>
                SoQLNull
              case Right((Left(_), Right(s))) =>
                SoQLUrl(None, Some(s))
              case Right((Right(s), Left(_))) =>
                SoQLUrl(Some(s), None)
              case Right((Right(s1), Right(s2))) =>
                SoQLUrl(Some(s1), Some(s2))
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    }
  )
}
