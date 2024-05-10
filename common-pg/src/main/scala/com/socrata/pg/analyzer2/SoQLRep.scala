package com.socrata.pg.analyzer2

import java.sql.{ResultSet, PreparedStatement, Types}

import com.rojoma.json.v3.ast.{JNull, JValue, JString}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.util.OrJNull.implicits._
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.joda.time.Period
import org.postgresql.util.PGInterval

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types._
import com.socrata.soql.sqlizer._

abstract class SoQLRepProvider[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({type ColumnType = SoQLType; type ColumnValue = SoQLValue; type DatabaseColumnNameImpl = String})](
  cryptProviders: CryptProviderProvider,
  override val exprSqlFactory: ExprSqlFactory[MT],
  override val namespace: SqlNamespaces[MT],
  override val toProvenance: types.ToProvenance[MT],
  override val isRollup: types.DatabaseTableName[MT] => Boolean,
  locationSubcolumns: Map[types.DatabaseTableName[MT], Map[types.DatabaseColumnName[MT], Seq[Option[types.DatabaseColumnName[MT]]]]],
  physicalTableFor: Map[AutoTableLabel, types.DatabaseTableName[MT]]
) extends Rep.Provider[MT] {
  def apply(typ: SoQLType) = reps(typ)

  override def mkTextLiteral(s: String): Doc =
    d"text" +#+ mkStringLiteral(s)
  override def mkByteaLiteral(bytes: Array[Byte]): Doc =
    d"bytea" +#+ mkStringLiteral(bytes.iterator.map { b => "%02x".format(b & 0xff) }.mkString("\\x", "", ""))

  private def createTextlikeIndices(idxBaseName: DocNothing, tableName: DatabaseTableName, colName: DocNothing) = {
    val databaseName = namespace.databaseTableName(tableName)
    Seq(
      d"CREATE INDEX IF NOT EXISTS" +#+ idxBaseName ++ d"_u" +#+ d"ON" +#+ databaseName +#+ d"(upper(" ++ colName ++ d") text_pattern_ops)",
      d"CREATE INDEX IF NOT EXISTS" +#+ idxBaseName ++ d"_tpo" +#+ d"ON" +#+ databaseName +#+ d"(" ++ colName +#+ d"text_pattern_ops)",
      d"CREATE INDEX IF NOT EXISTS" +#+ idxBaseName ++ d"_nl" +#+ d"ON" +#+ databaseName +#+ d"(" ++ colName +#+ d"nulls last)",
      d"CREATE INDEX IF NOT EXISTS" +#+ idxBaseName ++ d"_unl" +#+ d"ON" +#+ databaseName +#+ d"(upper(" ++ colName ++ d") nulls last)"
    )
  }

  private def createSimpleIndices(idxName: DocNothing, tableName: DatabaseTableName, colName: DocNothing) = {
    Seq(
      d"CREATE INDEX IF NOT EXISTS" +#+ idxName +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"(" ++ colName ++ d")"
    )
  }

  abstract class GeometryRep[T <: Geometry](t: SoQLType with SoQLGeometryLike[T], ctor: T => CV, name: String) extends SingleColumnRep(t, d"geometry") {
    private val open = d"st_${name}fromwkb"

    override def literal(e: LiteralValue) = {
      val geo = downcast(e.value)
      exprSqlFactory(Seq(mkByteaLiteral(t.WkbRep(geo)), Geo.defaultSRIDLiteral).funcall(open), e)
    }

    protected def downcast(v: SoQLValue): T

    override def hasTopLevelWrapper = true
    override def wrapTopLevel(raw: ExprSql) = {
      assert(raw.typ == typ)
      exprSqlFactory(raw.compressed.sql.funcall(d"st_asbinary"), raw.expr)
    }

    override def convertToText(e: ExprSql): Option[ExprSql] =
      Some(exprSqlFactory(e.compressed.sql.funcall(d"st_astext"), e.expr))

    override def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
      Option(rs.getBytes(dbCol)).flatMap { bytes =>
        t.WkbRep.unapply(bytes) // TODO: this just turns invalid values into null, we should probably be noisier than that
      }.map(ctor).getOrElse(SoQLNull)
    }

    override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
      new IngressRep[MT] {
        override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
          cv match {
            case SoQLNull =>
              stmt.setNull(start, Types.VARCHAR)
            case other =>
              val geo = downcast(other)
              stmt.setString(start, t.EWktRep(geo, Geo.defaultSRID))
          }
          start + 1
        }
        override def csvify(cv: CV): Seq[Option[String]] = {
          cv match {
            case SoQLNull =>
              Seq(None)
            case other =>
              val geo = downcast(other)
              Seq(Some(t.EWktRep(geo, Geo.defaultSRID)))
          }
        }
        override def placeholders: Seq[Doc] = Seq(d"ST_GeomFromEWKT(?)")
        override def indices =
          Seq(
            d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"USING GIST (" ++ compressedDatabaseColumn(columnName) ++ d")"
          )
      }
  }

  private def badType(expected: String, value: CV): Nothing =
    throw new Exception(s"Bad type; expected $expected, got $value")

  val reps = Map[SoQLType, Rep](
    SoQLID -> new ProvenancedRep(SoQLID, d"bigint") {
      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLID]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
        )
      }

      override def compressedDatabaseType = d"jsonb"

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val Seq(provenance, value) =
          e match {
            case compressed: ExprSql.Compressed[MT] =>
              Seq(d"(" ++ compressed.sql ++ d") ->> 0", d"((" ++ compressed.sql ++ d") ->> 1) :: bigint")
            case expanded: ExprSql.Expanded[MT] =>
              expanded.sqls
          }
        val converted = Seq(
          d"'row'",
          Seq(
            Seq(provenance).funcall(d"pg_temp.soql_obfuscate"),
            value
          ).funcall(d"obfuscate")
        ).funcall(d"format_obfuscated")
        Some(exprSqlFactory(converted, e.expr))
      }

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLID]
        val rawFormatted = SoQLID.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(Provenance(s)) => mkTextLiteral(s)
          }
        val numLit =
          rawId.provenance.flatMap(cryptProviders.forProvenance) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLID.StringRep(cryptProvider)
              val SoQLID(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        exprSqlFactory(Seq(provenanceLit, numLit), e)
      }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol)).map(Provenance(_))
        val valueRaw = rs.getLong(dbCol + 1)

        if(rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLID(valueRaw)
          result.provenance = provenance
          result
        }
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLID(v)
                result.provenance = Some(Provenance(prov))
                result
              case Right((Left(JNull), v)) =>
                SoQLID(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          val needProv = isRollup(tableName)

          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            if(needProv) {
              cv match {
                case SoQLNull =>
                  stmt.setNull(start, Types.VARCHAR)
                  stmt.setNull(start + 1, Types.BIGINT)
                case id@SoQLID(value) =>
                  id.provenance match {
                    case Some(Provenance(prov)) => stmt.setString(start, prov)
                    case None => stmt.setNull(start, Types.VARCHAR)
                  }
                  stmt.setLong(start + 1, value)
                case other =>
                  badType("id", other)
              }
              start + 2
            } else {
              cv match {
                case SoQLNull => stmt.setNull(start, Types.BIGINT)
                case id@SoQLID(value) => stmt.setLong(start, value)
                case other => badType("id", other)
              }
              start + 1
            }
          }

          override def csvify(cv: CV): Seq[Option[String]] = {
            if(needProv) {
              cv match {
                case SoQLNull =>
                  Seq(None, None)
                case id@SoQLID(value) =>
                  Seq(id.provenance.map(_.value), Some(value.toString))
                case other =>
                  badType("id", other)
              }
            } else {
              cv match {
                case SoQLNull =>
                  Seq(None)
                case id@SoQLID(value) =>
                  Seq(Some(value.toString))
                case other =>
                  badType("id", other)
              }
            }
          }

          override def placeholders =
            if(needProv) Seq(d"?", d"?")
            else Seq(d"?")

          override def indices =
            if(needProv) {
              Seq(
                d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"(" ++ expandedDatabaseColumns(columnName).commaSep ++ d")"
              )
            } else {
              Seq(
                d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"(" ++ compressedDatabaseColumn(columnName) ++ d")"
              )
            }
        }
    },
    SoQLVersion -> new ProvenancedRep(SoQLVersion, d"bigint") {
      override def provenanceOf(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        Set(rawId.provenance)
      }

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"((" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1) :: bigint AS" +#+ dataName,
        )
      }

      override def compressedDatabaseType = d"jsonb"

      def convertToText(e: ExprSql): Option[ExprSql] = {
        val Seq(provenance, value) =
          e match {
            case compressed: ExprSql.Compressed[MT] =>
              Seq(d"(" ++ compressed.sql ++ d") ->> 0", d"((" ++ compressed.sql ++ d") ->> 1) :: bigint")
            case expanded: ExprSql.Expanded[MT] =>
              expanded.sqls
          }
        val converted = Seq(
          d"'rv'",
          Seq(
            Seq(provenance).funcall(d"pg_temp.soql_obfuscate"),
            value
          ).funcall(d"obfuscate")
        ).funcall(d"format_obfuscated")
        Some(exprSqlFactory(converted, e.expr))
      }

      override def literal(e: LiteralValue) = {
        val rawId = e.value.asInstanceOf[SoQLVersion]
        val rawFormatted = SoQLVersion.FormattedButUnobfuscatedStringRep(rawId)
        // ok, "rawFormatted" is the string as the user entered it.
        // Now we want to examine with the appropriate
        // CryptProvider...

        val provenanceLit =
          rawId.provenance match {
            case None => d"null :: text"
            case Some(Provenance(s)) => mkTextLiteral(s)
          }
        val numLit =
          rawId.provenance.flatMap(cryptProviders.forProvenance) match {
            case None =>
              Doc(rawId.value.toString) +#+ d":: bigint"
            case Some(cryptProvider) =>
              val idStringRep = new SoQLVersion.StringRep(cryptProvider)
              val SoQLVersion(num) = idStringRep.unapply(rawFormatted).get
              Doc(num.toString) +#+ d":: bigint"
          }

        exprSqlFactory(Seq(provenanceLit, numLit), e)
      }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        val provenance = Option(rs.getString(dbCol)).map(Provenance(_))
        val valueRaw = rs.getLong(dbCol + 1)

        if(rs.wasNull) {
          SoQLNull
        } else {
          val result = SoQLVersion(valueRaw)
          result.provenance = provenance
          result
        }
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, String], Long)](v) match {
              case Right((Right(prov), v)) =>
                val result = SoQLVersion(v)
                result.provenance = Some(Provenance(prov))
                result
              case Right((Left(JNull), v)) =>
                SoQLVersion(v)
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          val needProv = isRollup(tableName)

          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            if(needProv) {
              cv match {
                case SoQLNull =>
                  stmt.setNull(start, Types.VARCHAR)
                  stmt.setNull(start + 1, Types.BIGINT)
                case id@SoQLVersion(value) =>
                  id.provenance match {
                    case Some(Provenance(prov)) => stmt.setString(start, prov)
                    case None => stmt.setNull(start, Types.VARCHAR)
                  }
                  stmt.setLong(start + 1, value)
                case other =>
                  badType("version", other)
              }
              start + 2
            } else {
              cv match {
                case SoQLNull => stmt.setNull(start, Types.BIGINT)
                case id@SoQLVersion(value) => stmt.setLong(start, value)
                case other => badType("version", other)
              }
              start + 1
            }
          }

          override def csvify(cv: CV): Seq[Option[String]] = {
            if(needProv) {
              cv match {
                case SoQLNull =>
                  Seq(None, None)
                case id@SoQLVersion(value) =>
                  Seq(id.provenance.map(_.value), Some(value.toString))
                case other =>
                  badType("version", other)
              }
            } else {
              cv match {
                case SoQLNull =>
                  Seq(None)
                case id@SoQLVersion(value) =>
                  Seq(Some(value.toString))
                case other =>
                  badType("version", other)
              }
            }
          }

          override def placeholders =
            if(needProv) Seq(d"?", d"?")
            else Seq(d"?")

          override def indices =
            if(needProv) {
              Seq(
                d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"(" ++ expandedDatabaseColumns(columnName).commaSep ++ d")"
              )
            } else {
              Seq(
                d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"(" ++ compressedDatabaseColumn(columnName) ++ d")"
              )
            }
        }
    },

    // ATOMIC REPS

    SoQLText -> new SingleColumnRep(SoQLText, d"text") {
      override def literal(e: LiteralValue) = {
        val SoQLText(s) = e.value
        exprSqlFactory(mkTextLiteral(s), e)
      }

      def convertToText(e: ExprSql): Option[ExprSql] = Some(e)

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLText(t)
        }
      }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            cv match {
              case SoQLNull => stmt.setNull(start, Types.VARCHAR)
              case SoQLText(t) => stmt.setString(start, t)
              case other => badType("text", other)
            }
            start + 1
          }
          override def csvify(cv: CV): Seq[Option[String]] = {
            cv match {
              case SoQLNull => Seq(None)
              case SoQLText(t) => Seq(Some(t))
              case other => badType("text", other)
            }
          }
          override def placeholders = Seq(d"?")
          override def indices =
            createTextlikeIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLNumber -> new SingleColumnRep(SoQLNumber, d"numeric") {
      override def literal(e: LiteralValue) = {
        val SoQLNumber(n) = e.value
        exprSqlFactory(Doc(n.toString) +#+ d"::" +#+ sqlType, e)
      }

      def convertToText(e: ExprSql): Option[ExprSql] =
        Some(exprSqlFactory(d"(" ++ e.compressed.sql ++ d") :: text", e.expr))

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getBigDecimal(dbCol)) match {
          case None => SoQLNull
          case Some(t) => SoQLNumber(t)
        }
      }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            cv match {
              case SoQLNull => stmt.setNull(start, Types.NUMERIC)
              case SoQLNumber(n) => stmt.setBigDecimal(start, n)
              case other => badType("number", other)
            }
            start + 1
          }
          override def csvify(cv: CV): Seq[Option[String]] = {
            cv match {
              case SoQLNull => Seq(None)
              case SoQLNumber(n) => Seq(Some(n.toString))
              case other => badType("number", other)
            }
          }
          override def placeholders = Seq(d"?")
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLBoolean -> new SingleColumnRep(SoQLBoolean, d"boolean") {
      def literal(e: LiteralValue) = {
        val SoQLBoolean(b) = e.value
        exprSqlFactory(if(b) d"true" else d"false", e)
      }

      def convertToText(e: ExprSql): Option[ExprSql] =
        // precise control over how booleans get stringified
        Some(exprSqlFactory(d"case (" ++ e.compressed.sql ++ d") when true then 'true' when false then 'false' end", e.expr))

      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        val v = rs.getBoolean(dbCol)
        if(rs.wasNull) {
          SoQLNull
        } else {
          SoQLBoolean(v)
        }
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            cv match {
              case SoQLNull => stmt.setNull(start, Types.BOOLEAN)
              case SoQLBoolean(b) => stmt.setBoolean(start, b)
              case other => badType("boolean", other)
            }
            start + 1
          }
          override def csvify(cv: CV): Seq[Option[String]] = {
            cv match {
              case SoQLNull => Seq(None)
              case SoQLBoolean(b) => Seq(Some(if(b) "true" else "false"))
              case other => badType("boolean", other)
            }
          }
          override def placeholders = Seq(d"?")
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLFixedTimestamp -> new SingleColumnRep(SoQLFixedTimestamp, d"timestamp with time zone") {
      def literal(e: LiteralValue) = {
        val SoQLFixedTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFixedTimestamp.StringRep(s)), e)
      }

      def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted =
          Seq(e.compressed.sql).funcall(d"soql_fixed_timestamp_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.FixedTimestampRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV): Seq[Option[String]] = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLFloatingTimestamp -> new SingleColumnRep(SoQLFloatingTimestamp, d"timestamp without time zone") {
      def literal(e: LiteralValue) = {
        val SoQLFloatingTimestamp(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLFloatingTimestamp.StringRep(s)), e)
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted =
          Seq(e.compressed.sql).funcall(d"soql_floating_timestamp_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.FloatingTimestampRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV): Seq[Option[String]] = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLDate -> new SingleColumnRep(SoQLDate, d"date") {
      def literal(e: LiteralValue) = {
        val SoQLDate(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLDate.StringRep(s)), e)
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted =
          Seq(e.compressed.sql).funcall(d"soql_date_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.DateRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV) = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLTime -> new SingleColumnRep(SoQLTime, d"time without time zone") {
      def literal(e: LiteralValue) = {
        val SoQLTime(s) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(SoQLTime.StringRep(s)), e)
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted =
          Seq(e.compressed.sql).funcall(d"soql_time_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.TimeRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV) = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },
    SoQLJson -> new SingleColumnRep(SoQLJson, d"jsonb") {
      def literal(e: LiteralValue) = {
        val SoQLJson(j) = e.value
        exprSqlFactory(sqlType +#+ mkStringLiteral(CompactJsonWriter.toString(j)), e)
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted = d"(" ++ e.compressed.sql ++ d""") :: text"""
        Some(exprSqlFactory(converted, e.expr))
      }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.JsonRep("")
      protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV) = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices =
            Seq(
              d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName) +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"USING GIN (" ++ compressedDatabaseColumn(columnName) ++ d")"
            )
        }
    },

    SoQLDocument -> new SingleColumnRep(SoQLDocument, d"jsonb") {
      override def literal(e: LiteralValue) = ??? // no such thing as a doc liteal

      // TODO: This isn't a very useful text representation
      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted = d"(" ++ e.compressed.sql ++ d""") :: text"""
        Some(exprSqlFactory(converted, e.expr))
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
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
      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            cv match {
              case SoQLNull => stmt.setNull(start, Types.VARCHAR)
              case SoQLJson(j) => stmt.setString(start, CompactJsonWriter.toString(j))
              case other => badType("json", other)
            }
            start + 1
          }
          override def csvify(cv: CV) = {
            cv match {
              case SoQLNull => Seq(None)
              case SoQLJson(j) => Seq(Some(CompactJsonWriter.toString(j)))
              case other => badType("json", other)
            }
          }
          override def placeholders = Seq(d"? ::" +#+ sqlType)
          override def indices = Nil
        }
    },

    SoQLInterval -> new SingleColumnRep(SoQLInterval, d"interval") {
      override def literal(e: LiteralValue) = {
        val SoQLInterval(p) = e.value
        exprSqlFactory(d"interval" +#+ mkStringLiteral(SoQLInterval.StringRep(p)), e)
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val converted = Seq(e.compressed.sql).funcall(d"soql_interval_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      override protected def doExtractFrom(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getObject(dbCol).asInstanceOf[PGInterval]) match {
          case Some(pgInterval) =>
            val period = new Period(pgInterval.getYears, pgInterval.getMonths, 0, pgInterval.getDays, pgInterval.getHours, pgInterval.getMinutes, pgInterval.getWholeSeconds, pgInterval.getMicroSeconds / 1000)
            SoQLInterval(period)
          case None =>
            SoQLNull
        }
      }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            cv match {
              case SoQLNull => stmt.setObject(start, new PGInterval())
              case SoQLInterval(i) => stmt.setObject(start, new PGInterval(i.getYears, i.getMonths, i.getDays, i.getHours, i.getMinutes, i.getSeconds * 1000.0 + i.getMillis))
              case other => badType("interval", other)
            }
            start + 1
          }
          override def csvify(cv: CV) = {
            cv match {
              case SoQLNull => Seq(None)
              case SoQLInterval(i) => Seq(Some(SoQLInterval.StringRep(i)))
              case other => badType("interval", other)
            }
          }
          override def placeholders = Seq(d"?")
          override def indices =
            createSimpleIndices(namespace.indexName(tableName, columnName), tableName, compressedDatabaseColumn(columnName))
        }
    },

    SoQLPoint -> new GeometryRep(SoQLPoint, SoQLPoint(_), "point") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPoint].value
    },
    SoQLMultiPoint -> new GeometryRep(SoQLMultiPoint, SoQLMultiPoint(_), "mpoint") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPoint].value
      override def isPotentiallyLarge = true
    },
    SoQLLine -> new GeometryRep(SoQLLine, SoQLLine(_), "line") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLLine].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiLine -> new GeometryRep(SoQLMultiLine, SoQLMultiLine(_), "mline") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiLine].value
      override def isPotentiallyLarge = true
    },
    SoQLPolygon -> new GeometryRep(SoQLPolygon, SoQLPolygon(_), "polygon") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLPolygon].value
      override def isPotentiallyLarge = true
    },
    SoQLMultiPolygon -> new GeometryRep(SoQLMultiPolygon, SoQLMultiPolygon(_), "mpoly") {
      override def downcast(v: SoQLValue) = v.asInstanceOf[SoQLMultiPolygon].value
      override def isPotentiallyLarge = true
    },

    // COMPOUND REPS

    SoQLPhone -> new CompoundColumnRep(SoQLPhone) {
      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)

      override def physicalColumnCount = 2

      override def physicalDatabaseColumns(name: ColumnLabel) = {
        Seq(namespace.columnName(name, "number"), namespace.columnName(name, "type"))
      }

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val subcolumns =
          e match {
            case expanded: ExprSql.Expanded[MT] => expanded.sqls
            case compressed: ExprSql.Compressed[MT] => Seq(compressed.sql)
          }

        val converted = subcolumns.funcall(d"soql_phone_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      override def physicalDatabaseTypes = Seq(d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(provenancedName, dataName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ provenancedName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ dataName,
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnName(name)

      override def compressedDatabaseType = d"jsonb"

      override def literal(e: LiteralValue) = {
        val ph@SoQLPhone(_, _) = e.value

        ph match {
          case SoQLPhone(None, None) =>
            exprSqlFactory(d"null :: jsonb", e)
          case SoQLPhone(phNum, phTyp) =>
            val numberLit = phNum match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val typLit = phTyp match {
              case Some(t) => mkTextLiteral(t)
              case None => d"null :: text"
            }

            exprSqlFactory(d"jsonb_build_array(" ++ numberLit ++ d"," ++ typLit ++ d")", e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "phone_number" => SubcolInfo[MT](SoQLPhone, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "phone_type" => SubcolInfo[MT](SoQLPhone, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.PhoneRep("")
      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
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

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV) = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"?", d"?")
          override def indices = {
            val Seq(numberCol, typeCol) = expandedDatabaseColumns(columnName)
            createTextlikeIndices(namespace.indexName(tableName, columnName, "number"), tableName, numberCol) ++
              createTextlikeIndices(namespace.indexName(tableName, columnName, "type"), tableName, typeCol)
          }
        }
    },

    SoQLLocation -> new CompoundColumnRep(SoQLLocation) {
      override def physicalColumnRef(col: PhysicalColumn) = {
        val colInfo: Seq[Option[DatabaseColumnName]] = locationSubcolumns(physicalTableFor(col.table))(col.column)

        exprSqlFactory(
          (namespace.tableLabel(col.table) ++ d"." ++ compressedDatabaseColumn(col.column)) +:
            colInfo.map {
              case Some(DatabaseColumnName(dcn)) =>
                (namespace.tableLabel(col.table) ++ d"." ++ Doc(dcn))
              case None =>
                d"null :: text"
            },
          col)
      }

      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: geometry", d"null :: text", d"null :: text", d"null :: text", d"null :: text"), e)

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val subcolumns =
          e match {
            case expanded: ExprSql.Expanded[MT] => expanded.sqls
            case compressed: ExprSql.Compressed[MT] => Seq(compressed.sql)
          }

        val converted = subcolumns.funcall(d"soql_location_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      override def physicalColumnCount = 5

      override def physicalDatabaseTypes = Seq(d"geometry", d"text", d"text", d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(ptName, addressName, cityName, stateName, zipName) = expandedDatabaseColumns(column)
        Seq(
          d"soql_extract_compressed_location_point(" ++ Doc(table) ++ d"." ++ sourceName ++ d") AS" +#+ ptName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ addressName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 2 AS" +#+ cityName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 3 AS" +#+ stateName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 4 AS" +#+ zipName,
        )
      }

      override def physicalDatabaseColumns(name: ColumnLabel) = {
        Seq(
          namespace.columnName(name),
          namespace.columnName(name, "address"),
          namespace.columnName(name, "city"),
          namespace.columnName(name, "state"),
          namespace.columnName(name, "zip")
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnName(name)

      override def compressedDatabaseType = d"jsonb"

      override def literal(e: LiteralValue) = {
        val loc@SoQLLocation(_, _, _) = e.value
        ??? // No such thing as a location literal
      }

      override def subcolInfo(field: String) =
        field match {
          case "point" => SubcolInfo[MT](SoQLLocation, 0, "geometry", SoQLPoint, { e => Seq(e.parenthesized).funcall(d"soql_extract_compressed_location_point") })
          case "address" => SubcolInfo[MT](SoQLLocation, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
          case "city" => SubcolInfo[MT](SoQLLocation, 2, "text", SoQLText, _.parenthesized +#+ d"->> 2")
          case "state" => SubcolInfo[MT](SoQLLocation, 3, "text", SoQLText, _.parenthesized +#+ d"->> 3")
          case "zip" => SubcolInfo[MT](SoQLLocation, 4, "text", SoQLText, _.parenthesized +#+ d"->> 4")
        }

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ??? // can't ingress location
          }
          override def csvify(cv: CV) = {
            ??? // can't ingress location
          }
          override def placeholders =
            ??? // can't ingress location

          override def indices = {
            // you _can_ have location indexes, if the location is part of a rollup
            val Seq(pointCol, addressCol, cityCol, stateCol, zipCol) = expandedDatabaseColumns(columnName)

            val textIndices =
              createTextlikeIndices(namespace.indexName(tableName, columnName, "address"), tableName, addressCol) ++
                createTextlikeIndices(namespace.indexName(tableName, columnName, "city"), tableName, cityCol) ++
                createTextlikeIndices(namespace.indexName(tableName, columnName, "state"), tableName, stateCol) ++
                createTextlikeIndices(namespace.indexName(tableName, columnName, "zip"), tableName, zipCol)

            textIndices ++ Seq(
              d"CREATE INDEX IF NOT EXISTS" +#+ namespace.indexName(tableName, columnName, "point") +#+ d"ON" +#+ namespace.databaseTableName(tableName) +#+ d"USING GIST (" ++ pointCol ++ d")"
            )
          }
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
            exprSqlFactory(Seq(sqls.head).funcall(d"st_asbinary") +: sqls.tail, expanded.expr)
        }
      }

      implicit class ToBigDecimalAugmentation(val x: Double) {
        def toBigDecimal = java.math.BigDecimal.valueOf(x)
      }

      private def locify(point: Option[Point], address: Option[String], city: Option[String], state: Option[String], zip: Option[String]): SoQLValue =
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
              // Building this as a string rather than a JValue to keep the formatting exactly the same as before
              s"""{"address": ${JString(address.getOrElse(""))}, "city": ${JString(city.getOrElse(""))}, "state": ${JString(state.getOrElse(""))}, "zip": ${JString(zip.getOrElse(""))}}"""
            )
          )
        }

      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        locify(
          Option(rs.getBytes(dbCol)).flatMap { bytes =>
            SoQLPoint.WkbRep.unapply(bytes)
          },
          Option(rs.getString(dbCol+1)),
          Option(rs.getString(dbCol+2)),
          Option(rs.getString(dbCol+3)),
          Option(rs.getString(dbCol+4))
        )
      }

      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
        Option(rs.getString(dbCol)) match {
          case None =>
            SoQLNull
          case Some(v) =>
            JsonUtil.parseJson[(Either[JNull, JValue], Either[JNull, String], Either[JNull, String], Either[JNull, String], Either[JNull, String])](v) match {
              case Right((pt, address, city, state, zip)) =>
                locify(
                  pt.toOption.flatMap { ptJson => SoQLPoint.JsonRep.unapply(ptJson.toString) },
                  address.toOption,
                  city.toOption,
                  state.toOption,
                  zip.toOption
                )
              case Left(err) =>
                throw new Exception(err.english)
            }
        }
      }
    },

    SoQLUrl -> new CompoundColumnRep(SoQLUrl) {
      override def nullLiteral(e: NullLiteral) =
        exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)

      override def convertToText(e: ExprSql): Option[ExprSql] = {
        val subcolumns =
          e match {
            case expanded: ExprSql.Expanded[MT] => expanded.sqls
            case compressed: ExprSql.Compressed[MT] => Seq(compressed.sql)
          }

        val converted = subcolumns.funcall(d"soql_url_to_text")
        Some(exprSqlFactory(converted, e.expr))
      }

      override def physicalColumnCount = 2

      override def physicalDatabaseColumns(name: ColumnLabel) = {
        Seq(namespace.columnName(name, "url"), namespace.columnName(name, "description"))
      }

      override def physicalDatabaseTypes = Seq(d"text", d"text")

      override def compressedSubColumns(table: String, column: ColumnLabel) = {
        val sourceName = compressedDatabaseColumn(column)
        val Seq(urlName, descName) = expandedDatabaseColumns(column)
        Seq(
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 0 AS" +#+ urlName,
          d"(" ++ Doc(table) ++ d"." ++ sourceName ++ d") ->> 1 AS" +#+ descName,
        )
      }

      override def compressedDatabaseColumn(name: ColumnLabel) =
        namespace.columnName(name)

      override def compressedDatabaseType = d"jsonb"

      override def literal(e: LiteralValue) = {
        val url@SoQLUrl(_, _) = e.value

        url match {
          case SoQLUrl(None, None) =>
            exprSqlFactory(Seq(d"null :: text", d"null :: text"), e)
          case SoQLUrl(urlUrl, urlDesc) =>
            val urlLit = urlUrl match {
              case Some(n) => mkTextLiteral(n)
              case None => d"null :: text"
            }
            val descLit = urlDesc match {
              case Some(t) => mkTextLiteral(t)
              case None => d"null :: text"
            }

            exprSqlFactory(Seq(urlLit, descLit), e)
        }
      }

      override def subcolInfo(field: String) =
        field match {
          case "url" => SubcolInfo[MT](SoQLUrl, 0, "text", SoQLText, _.parenthesized +#+ d"->> 0")
          case "description" => SubcolInfo[MT](SoQLUrl, 1, "text", SoQLText, _.parenthesized +#+ d"->> 1")
        }

      private val ugh = new com.socrata.datacoordinator.common.soql.sqlreps.UrlRep("")
      override protected def doExtractExpanded(rs: ResultSet, dbCol: Int): CV = {
        ugh.fromResultSet(rs, dbCol)
      }
      override protected def doExtractCompressed(rs: ResultSet, dbCol: Int): CV = {
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

      override def ingressRep(tableName: DatabaseTableName, columnName: ColumnLabel) =
        new IngressRep[MT] {
          override def populatePreparedStatement(stmt: PreparedStatement, start: Int, cv: CV): Int = {
            ugh.prepareInsert(stmt, cv, start)
          }
          override def csvify(cv: CV) = {
            ugh.csvifyForInsert(cv)
          }
          override def placeholders = Seq(d"?", d"?")
          override def indices = {
            val Seq(urlCol, descCol) = expandedDatabaseColumns(columnName)
            createTextlikeIndices(namespace.indexName(tableName, columnName, "url"), tableName, urlCol) ++
              createTextlikeIndices(namespace.indexName(tableName, columnName, "desc"), tableName, descCol)
          }
        }
    }
  )
}
