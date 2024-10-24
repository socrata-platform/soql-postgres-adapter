package com.socrata.pg.soql

import scala.collection.mutable
import java.sql.PreparedStatement

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.environment.{ResourceName, TableName}
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext.SqlizerContext

case class ParametricSql(sql: Seq[String], setParams: Seq[SetParam]) {
  override def toString(): String = {
    val params = setParams.map { (setParam) => setParam(None, 0).get }
    "sql: " + sql.mkString(";") +
    " params: " + params.mkString("\"", """","""", "\"")
  }

  def paramsAsStrings: Seq[String] =
    setParams.map { (setParam) => setParam(None, 0).get.toString }
}

// scalastyle:off import.grouping
trait Sqlizer[T] {
  import Sqlizer._
  import SqlizerContext._

  def sql(e: T)
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam], ctx: Context, escape: Escape): ParametricSql

  protected def useUpper(e: T)(ctx: Context): Boolean = {
    if (caseInsensitive(ctx)) {
      ctx(SoqlPart) match {
        case SoqlWhere | SoqlGroup | SoqlOrder | SoqlHaving => true
        case SoqlSelect => usedInGroupBy(e)(ctx)
        case SoqlSearch => false
        case _ => false
      }
    } else { false }
  }

  protected def usedInGroupBy(e: T)(ctx: Context): Boolean = {
    val rootExpr = ctx.get(RootExpr)
    ctx(SoqlPart) match {
      case SoqlSelect | SoqlOrder =>
        ctx.get(Analysis) match {
          case Some(analysis: SoQLAnalysis[_, _]) =>
            analysis.groupBys.exists(expr => (e == expr) || rootExpr.exists(_ == expr))
          case _ => false
        }
      case SoqlSearch => false
      case _ => false
    }
  }

  /**
   * Newly introduced selected expression in sub-soql needs aliases for chained soql.
   */
  protected def selectAlias(e: CoreExpr[_, _], subColumn: Option[String] = None)(ctx: Context): String = {
    ctx(SoqlPart) match {
      case SoqlSelect if ((true != ctx(OutermostSoql) || ctx.contains(IsSubQuery)) && e == ctx(RootExpr)) =>
        // Geometry types require ST_AsBinary and does not work well with aliases
        // Normally, aliases is not needed in the outermost soql.
        val alias = ctx(SqlizerContext.ColumnName).asInstanceOf[String] + subColumn.getOrElse("")
        " as \"%s\"".format(internAlias(ctx, alias).replace("\"", "\"\""))
      case _ =>
        ""
    }
  }

  def internAlias(ctx: Context, alias: String): String = {
    ctx(SqlizerContext.NameCache).asInstanceOf[Sqlizer.NameCache].intern(alias)
  }

  protected val ParamPlaceHolder: String = "?"

  private def caseInsensitive(ctx: Context): Boolean =
    ctx.contains(CaseSensitivity) && ctx(CaseSensitivity) == CaseInsensitive

  protected def realAlias(tableName: TableName, realTableName: String): String = {
    tableName.alias match {
      case Some(x) => ResourceName(x).caseFolded
      case None => realTableName
    }
  }
}

object Sqlizer {

  type Context = Map[SqlizerContext, Any]

  type SetParam = (Option[PreparedStatement], Int) => Option[Any]

  def sql[T](e: T)(rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Context,
                   escape: Escape)(implicit ev: Sqlizer[T]): ParametricSql = {
    ev.sql(e)(rep, typeRep, setParams, ctx, escape)
  }

  class NameCache {
    private val cache = new mutable.HashMap[String, String]

    private def unquote(s: String) =
      // sql crimes.  This is so bad, mitigated only by the fact that
      // SoQL aliases have to be valid identifiers - quoting in SoQL
      // is _only_ to avoid collisions with reserved words, and
      // therefore there can be no quoting mischief happening.  This
      // is only necessary because sometimes we're dealing with quoted
      // names and sometimes we're not, and there's no indication
      // which is which, sigh.
      if(s.startsWith("\"") && s.endsWith("\"")) {
        s.drop(1).dropRight(1).replace("\"\"", "\"")
      } else {
        s
      }

    def intern(rawName: String) = {
      val normalizedName = unquote(rawName)
      cache.get(normalizedName) match {
        case Some(name) =>
          name
        case None if normalizedName.length() > 63 || normalizedName.matches("^[Ii][0-9]+$") =>
          // the "i" prefix is not used by either rollup tables (which
          // use "c") or database tables (which use "s" and "u").
          // Note this isn't actually correct _in general_ but the
          // places it is not correct are tests (which don't follow
          // the actual ttable column naming scheme).  They also don't
          // create columns named "i###" though.
          //
          // We're force-renaming columns that already look like this
          // in order to make `... join (select i0, x as
          // some_very_long_alias) ...` work correctly without needing
          // to know locally that "i0" was taken as a physical table
          // name.
          val name = s"i${cache.size}"
          cache += normalizedName -> name
          name
        case None =>
          rawName
      }
    }
  }

  implicit val stringLiteralSqlizer = StringLiteralSqlizer

  implicit val numberLiteralSqlizer = NumberLiteralSqlizer

  implicit val functionCallSqlizer = FunctionCallSqlizer

  implicit val binaryTreeSoqlAnalysisSqlizer = BinarySoQLAnalysisSqlizer

  implicit object CoreExprSqlizer extends Sqlizer[CoreExpr[UserColumnId, SoQLType]] {
    def sql(expr: CoreExpr[UserColumnId, SoQLType])(rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                                    typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                                    setParams: Seq[SetParam],
                                                    ctx: Context,
                                                    escape: Escape): ParametricSql = {
      expr match {
        case fc: FunctionCall[UserColumnId, SoQLType] =>
          FunctionCallSqlizer.sql(fc)(rep, typeRep, setParams, ctx, escape)
        case cr: ColumnRef[UserColumnId, SoQLType] => ColumnRefSqlizer.sql(cr)(rep, typeRep, setParams, ctx, escape)
        case lit: StringLiteral[SoQLType] => StringLiteralSqlizer.sql(lit)(rep, typeRep, setParams, ctx, escape)
        case lit: NumberLiteral[SoQLType] => NumberLiteralSqlizer.sql(lit)(rep, typeRep, setParams, ctx, escape)
        case lit: BooleanLiteral[SoQLType] => BooleanLiteralSqlizer.sql(lit)(rep, typeRep, setParams, ctx, escape)
        case lit: NullLiteral[SoQLType] => NullLiteralSqlizer.sql(lit)(rep, typeRep, setParams, ctx, escape)
      }
    }
  }

  implicit object OrderBySqlizer extends Sqlizer[OrderBy[UserColumnId, SoQLType]] {
    def sql(orderBy: OrderBy[UserColumnId, SoQLType])
           (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
            typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
            setParams: Seq[SetParam],
            ctx: Context,
            escape: Escape): ParametricSql = {
      val ParametricSql(ss, setParamsOrderBy) = Sqlizer.sql(orderBy.expression)(rep, typeRep, setParams, ctx, escape)
      val se = ss.map { s =>
        s + (if (orderBy.ascending) "" else " desc") + (if (orderBy.nullLast) " nulls last" else "") }
      ParametricSql(se, setParamsOrderBy)
    }
  }

  def isLiteral(expr: CoreExpr[UserColumnId, SoQLType]): Boolean = {
    expr match {
      case cr: ColumnRef[UserColumnId, SoQLType] => false
      case lit: TypedLiteral[SoQLType] => true
      case fc: FunctionCall[UserColumnId, SoQLType] =>
        !fc.function.isAggregate &&
          fc.parameters.nonEmpty && // kinda questionable
          fc.parameters.forall(x => isLiteral(x))
    }
  }

  def isImmediateLiteral(expr: CoreExpr[UserColumnId, SoQLType]): Boolean = {
    expr match {
      case lit: TypedLiteral[SoQLType] => true
      case _ => false
    }
  }
}

object SqlizerContext extends Enumeration {
  type SqlizerContext = Value
  val NameCache = Value("name-cache")
  val Analysis = Value("analysis")
  val SoqlPart = Value("soql-part")
  val SoqlSelect = Value("select")
  val SoqlWhere = Value("where")
  val SoqlJoin = SoqlWhere // join is like where
  val SoqlGroup = Value("group")
  val SoqlHaving = Value("having")
  val SoqlOrder = Value("order")
  val SoqlSearch = Value("search")
  val SoqlDistinct = Value("distinct")
  // Normally geometry is converted to text when used in select.
  // But if the sql is used for table insert like rollup, we do not want geometry converted to text.
  val LeaveGeomAsIs = Value("leave-geom-as-is")
  val IdRep = Value("id-rep")
  val VerRep = Value("ver-rep")
  val RootExpr = Value("root-expr")
  val ColumnName = Value("column-name")
  val CaseSensitivity = Value("case-sensitivity")
  val InnermostSoql = Value("innermost-soql")
  val OutermostSoql = Value("outermost-soql")
  val OutermostSoqls = Value("outermost-soqls")

  val TableMap = Value("tables") // resource name to table name map
  val TableAliasMap = Value("table-aliases") // resource alias name to table name map
  val SimpleJoinMap = Value("simple-joins")

  val PrimaryTableAlias = Value("primary-table-alias")

  val IsSubQuery = Value("is-sub-query")
  val InsideWindowFn = Value("inside-window-fn")
  val LeadingSearch = Value("leading-search")

  val TimestampLiteral = Value("timestamp-literal")
  val SoQLContext = Value("soql-context")
  val ObfuscationKeySql = Value("obfuscation-key-sql")
}

sealed trait CaseSensitivity
object CaseInsensitive extends CaseSensitivity
object CaseSensitive extends CaseSensitivity
