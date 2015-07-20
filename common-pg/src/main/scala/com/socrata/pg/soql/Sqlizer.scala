package com.socrata.pg.soql

import java.sql.PreparedStatement
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext.SqlizerContext

case class ParametricSql(sql: String, setParams: Seq[SetParam])

trait Sqlizer[T] {

  import Sqlizer._
  import SqlizerContext._

  def sql(e: T)(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam], ctx: Context, escape: Escape): ParametricSql

  protected def useUpper(e: T)(ctx: Context): Boolean = {
    if (caseInsensitive(ctx))
      ctx(SoqlPart) match {
        case SoqlWhere | SoqlGroup | SoqlOrder | SoqlHaving => true
        case SoqlSelect => usedInGroupBy(e)(ctx)
        case SoqlSearch => false
        case _ => false
      }
    else false
  }

  protected def usedInGroupBy(e: T)(ctx: Context): Boolean = {
    val rootExpr = ctx.get(RootExpr)
    ctx(SoqlPart) match {
      case SoqlSelect | SoqlOrder =>
        ctx.get(Analysis) match {
          case Some(analysis: SoQLAnalysis[_, _]) =>
            analysis.groupBy match {
              case Some(groupBy) =>
                // Use upper in select if this expression or the selected expression it belongs to is found in group by
                groupBy.exists(expr => (e == expr) || rootExpr.exists(_ == expr))
              case None => false
            }
          case _ => false
        }
      case SoqlSearch => false
      case _ => false
    }
  }

  protected val ParamPlaceHolder: String = "?"

  private def caseInsensitive(ctx: Context): Boolean =
    ctx.contains(CaseSensitivity) && ctx(CaseSensitivity) == CaseInsensitive
}

object Sqlizer {

  type Context = Map[SqlizerContext, Any]

  type SetParam = (Option[PreparedStatement], Int) => Option[Any]

  def sql[T](e: T)
            (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context, escape: Escape)
            (implicit ev: Sqlizer[T]): ParametricSql = {
    ev.sql(e)(rep, setParams, ctx, escape)
  }

  implicit val stringLiteralSqlizer = StringLiteralSqlizer

  implicit val numberLiteralSqlizer = NumberLiteralSqlizer

  implicit val functionCallSqlizer = FunctionCallSqlizer

  implicit val soqlAnalysisSqlizer = SoQLAnalysisSqlizer

  implicit object CoreExprSqlizer extends Sqlizer[CoreExpr[UserColumnId, SoQLType]] {
    def sql(expr: CoreExpr[UserColumnId, SoQLType])(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context, escape: Escape)
    : ParametricSql = {
      expr match {
        case fc: FunctionCall[UserColumnId, SoQLType] => FunctionCallSqlizer.sql(fc)(rep, setParams, ctx, escape)
        case cr: ColumnRef[UserColumnId, SoQLType] => ColumnRefSqlizer.sql(cr)(rep, setParams, ctx, escape)
        case lit: StringLiteral[SoQLType] => StringLiteralSqlizer.sql(lit)(rep, setParams, ctx, escape)
        case lit: NumberLiteral[SoQLType] => NumberLiteralSqlizer.sql(lit)(rep, setParams, ctx, escape)
        case lit: BooleanLiteral[SoQLType] => BooleanLiteralSqlizer.sql(lit)(rep, setParams, ctx, escape)
        case lit: NullLiteral[SoQLType] => NullLiteralSqlizer.sql(lit)(rep, setParams, ctx, escape)
      }
    }
  }

  implicit object OrderBySqlizer extends Sqlizer[OrderBy[UserColumnId, SoQLType]] {

    def sql(orderBy: OrderBy[UserColumnId, SoQLType])
           (rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context, escape: Escape) = {
      val ParametricSql(s, setParamsOrderBy) = Sqlizer.sql(orderBy.expression)(rep, setParams, ctx, escape)
      val se = s + (if (orderBy.ascending) "" else " desc") + (if (orderBy.nullLast) " nulls last" else "")
      ParametricSql(se, setParamsOrderBy)
    }
  }
}

object SqlizerContext extends Enumeration {
  type SqlizerContext = Value
  val Analysis = Value("analysis")
  val SoqlPart = Value("soql-part")
  val SoqlSelect = Value("select")
  val SoqlWhere = Value("where")
  val SoqlGroup = Value("group")
  val SoqlHaving = Value("having")
  val SoqlOrder = Value("order")
  val SoqlSearch = Value("search")
  // Normally geometry is converted to text when used in select.
  // But if the sql is used for table insert like rollup, we do not want geometry converted to text.
  val LeaveGeomAsIs = Value("leave-geom-as-is")
  val IdRep = Value("id-rep")
  val VerRep = Value("ver-rep")
  val RootExpr = Value("root-expr")
  val CaseSensitivity = Value("case-sensitivity")
}

sealed trait CaseSensitivity

object CaseInsensitive extends CaseSensitivity

object CaseSensitive extends CaseSensitivity