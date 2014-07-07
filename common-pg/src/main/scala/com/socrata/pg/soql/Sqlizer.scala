package com.socrata.pg.soql

import java.sql.PreparedStatement
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.pg.soql.Sqlizer.SetParam
import com.socrata.pg.soql.SqlizerContext.SqlizerContext


case class ParametricSql(sql: String, setParams: Seq[SetParam])

trait Sqlizer[T] {

  import Sqlizer._
  import SqlizerContext._

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], ctx: Context): ParametricSql

  val underlying: T

  protected def useUpper(ctx: Context): Boolean = {
    if (caseInsensitive(ctx))
      ctx(SoqlPart) match {
        case SoqlWhere | SoqlGroup | SoqlOrder | SoqlHaving => true
        case SoqlSelect => usedInGroupBy(ctx)
        case SoqlSearch => false
        case _ => false
      }
    else false
  }

  protected def usedInGroupBy(ctx: Context): Boolean = {
    val rootExpr = ctx.get(RootExpr)
    ctx(SoqlPart) match {
      case SoqlSelect | SoqlOrder =>
        ctx.get(Analysis) match {
          case Some(analysis: SoQLAnalysis[_, _]) =>
            analysis.groupBy match {
              case Some(groupBy) =>
                // Use upper in select if this expression or the selected expression it belongs to is found in group by
                groupBy.exists(expr => (underlying == expr) || rootExpr.exists(_ == expr))
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

  implicit def stringLiteralSqlizer(lit: StringLiteral[SoQLType]): Sqlizer[StringLiteral[SoQLType]] = {
    new StringLiteralSqlizer(lit)
  }

  implicit def functionCallSqlizer(lit: FunctionCall[UserColumnId, SoQLType]): Sqlizer[FunctionCall[UserColumnId, SoQLType]] = {
    new FunctionCallSqlizer(lit)
  }

  implicit def coreExprSqlizer(expr: CoreExpr[UserColumnId, SoQLType]): Sqlizer[_] = {
    expr match {
      case fc: FunctionCall[UserColumnId, SoQLType] => new FunctionCallSqlizer(fc)
      case cr: ColumnRef[UserColumnId, SoQLType] => new ColumnRefSqlizer(cr)
      case lit: StringLiteral[SoQLType] => new StringLiteralSqlizer(lit)
      case lit: NumberLiteral[SoQLType] => new NumberLiteralSqlizer(lit)
      case lit: BooleanLiteral[SoQLType] => new BooleanLiteralSqlizer(lit)
      case lit: NullLiteral[SoQLType] => new NullLiteralSqlizer(lit)
    }
  }

  implicit def orderBySqlizer(ob: OrderBy[UserColumnId, SoQLType]): Sqlizer[OrderBy[UserColumnId, SoQLType]] = {
    new OrderBySqlizer(ob)
  }

  implicit def analysisSqlizer(analysisTable: Tuple3[SoQLAnalysis[UserColumnId, SoQLType], String, Seq[SqlColumnRep[SoQLType, SoQLValue]]]) = {
    new SoQLAnalysisSqlizer(analysisTable._1, analysisTable._2, analysisTable._3)
  }

  def toGeoText(sql: String, typ: SoQLType, ctx: Context): String = {
    import SqlizerContext._
    if (GeoTypes.contains(typ) && ctx.get(SoqlPart) == Some(SoqlSelect)) s"ST_AsText($sql)"
    else sql
  }

  private val GeoTypes: Set[SoQLType] = Set(SoQLPoint, SoQLMultiLine, SoQLMultiPolygon)
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
  val IdRep = Value("id-rep")
  val VerRep = Value("ver-rep")
  val RootExpr = Value("root-expr")
  val CaseSensitivity = Value("case-sensitivity")
}

sealed trait CaseSensitivity

object CaseInsensitive extends CaseSensitivity

object CaseSensitive extends CaseSensitivity