package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.environment.TableName

import java.sql.PreparedStatement

import Sqlizer._
import SqlizerContext._

object StringLiteralSqlizer extends Sqlizer[StringLiteral[SoQLType]] {
  def sql(lit: StringLiteral[SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        val v = toUpper(lit, quote(lit.value, escape), ctx)
        ParametricSql(Seq(v), setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        val v = toUpper(lit, quote(lit.value, escape), ctx)
        ParametricSql(Seq(v), setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          val maybeUpperLitVal = toUpper(lit, lit.value, ctx)
          stmt.foreach(_.setString(pos, maybeUpperLitVal))
          Some(maybeUpperLitVal)
        }
        // Append value as comment for debug
        // val comment = " /* %s */".format(lit.value)
        ParametricSql(Seq(ParamPlaceHolder + selectAlias(lit)(ctx) /* + comment */), setParams :+ setParam)
    }
  }

  private def quote(s: String, escape: Escape) = s"e'${escape(s)}'"

  private def toUpper(lit: StringLiteral[SoQLType], v: String, ctx: Context): String =
    if (useUpper(lit)(ctx)) v.toUpperCase else v
}

object NumberLiteralSqlizer extends Sqlizer[NumberLiteral[SoQLType]] {
  def sql(lit: NumberLiteral[SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(Seq(lit.value.bigDecimal.toPlainString), setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        ParametricSql(Seq(lit.value.bigDecimal.toPlainString), setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          stmt.foreach(_.setBigDecimal(pos, lit.value.bigDecimal))
          Some(lit.value)
        }
        ParametricSql(Seq(ParamPlaceHolder + selectAlias(lit)(ctx)), setParams :+ setParam)
    }
  }
}

object BooleanLiteralSqlizer extends Sqlizer[BooleanLiteral[SoQLType]] {
  def sql(lit: BooleanLiteral[SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    ctx.get(SoqlPart) match {
      case Some(SoqlHaving) | Some(SoqlGroup) =>
        ParametricSql(Seq(lit.toString), setParams)
      case Some(SoqlSelect) | Some(SoqlOrder) if usedInGroupBy(lit)(ctx) =>
        ParametricSql(Seq(lit.toString), setParams)
      case _ =>
        val setParam = (stmt: Option[PreparedStatement], pos: Int) => {
          stmt.foreach(_.setBoolean(pos, lit.value))
          Some(lit.value)
        }
        ParametricSql(Seq(ParamPlaceHolder + selectAlias(lit)(ctx)), setParams :+ setParam)
    }
  }
}

object NullLiteralSqlizer extends Sqlizer[NullLiteral[SoQLType]] {
  def sql(lit: NullLiteral[SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql =
    ParametricSql(Seq("null" + selectAlias(lit)(ctx)), setParams)
}

object FunctionCallSqlizer extends Sqlizer[FunctionCall[UserColumnId, SoQLType]] {
  def sql(expr: FunctionCall[UserColumnId, SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val fn = SqlFunctions(expr.function.function)
    val ParametricSql(sqls, fnSetParams) = fn(expr, rep, typeRep, setParams, ctx, escape)
    // SoQL parsing bakes parenthesis into the ast tree without explicitly spitting out parenthesis.
    // We add parenthesis to every function call to preserve semantics.
    ParametricSql(sqls.map(s => s"($s)" + selectAlias(expr)(ctx)), fnSetParams)
  }
}

object ColumnRefSqlizer extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  private def idQuote(s: String) = s""""$s"""" //   "\"" + s + "\""

  // scalastyle:off cyclomatic.complexity
  def sql(expr: ColumnRef[UserColumnId, SoQLType])
         (reps: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val tableMap = ctx(TableAliasMap).asInstanceOf[Map[String, String]]
    reps.get(QualifiedUserColumnId(expr.qualifier, expr.column)) match {
      case Some(rep) if ctx(InnermostSoql) == true || expr.qualifier.nonEmpty => // scalastyle:off simplify.boolean.expression
        if (complexTypes.contains(expr.typ) &&
            ctx.get(SoqlPart).exists(_ == SoqlSelect) &&
            ctx.get(RootExpr).exists(_ == expr)) {
          val qualifer = tableMap.get(expr.qualifier.getOrElse(TableName.PrimaryTable.qualifier))
          val maybeUpperPhysColumns =
            rep.physColumns.zip(rep.sqlTypes).map { case (physCol, sqlType) =>
              toUpper(expr, sqlType)(qualifer.map(q => s"$q.$physCol").getOrElse(physCol), ctx)
            }
          val subColumns = rep.physColumns.map(pc => pc.replace(rep.base, ""))
          val physColumnsWithSubColumns = maybeUpperPhysColumns.zip(subColumns)
          val columnsWithAlias = physColumnsWithSubColumns.map { case (physCol, subCol) =>
            physCol + selectAlias(expr, Some(subCol))(ctx)
          }
          ParametricSql(columnsWithAlias, setParams)
        } else {
          val qualifer = tableMap.get(expr.qualifier.getOrElse(TableName.PrimaryTable.qualifier))
          val maybeUpperPhysColumns = rep.physColumns.map(c => toUpper(expr)(qualifer.map(q => s"$q.$c").getOrElse(c), ctx))
          ParametricSql(maybeUpperPhysColumns.map(c => c + selectAlias(expr)(ctx)), setParams)
        }
      case _ => // Outer soqls do not get rep by column id.  They get rep by datatype.
        val typeReps = typeRep ++
                       reps.values.map((rep: SqlColumnRep[SoQLType, SoQLValue]) => (rep.representedType -> rep)).toMap
        typeReps.get(expr.typ) match {
          case Some(rep) =>
            val subColumns = rep.physColumns.map { pc => pc.replace(rep.base, "") }
            val sqls = subColumns.map { subCol =>
              toUpper(expr)(idQuote(expr.column.underlying + subCol), ctx) + selectAlias(expr, Some(subCol))(ctx)
            }
            ParametricSql(sqls, setParams)
          case None =>
            val schema = reps.map { case (columnId, rep) => (columnId.userColumnId.underlying -> rep.representedType) }
            val soql = ctx.get(SoqlSelect).getOrElse("no select info")
            throw new Exception(s"cannot find rep for ${expr.column.underlying} ${expr.typ}\n$soql\n${schema.toString}")
        }
    }
  }

  private def toUpper(expr: ColumnRef[UserColumnId, SoQLType])(phyColumn: String, ctx: Context): String =
    if (expr.typ == SoQLText && useUpper(expr)(ctx)) s"upper($phyColumn)" else phyColumn

  private def toUpper(expr: ColumnRef[UserColumnId, SoQLType], sqlType: String)(phyColumn: String, ctx: Context)
    : String = {
    if (sqlType == "TEXT" && useUpper(expr)(ctx)) s"upper($phyColumn)" else phyColumn
  }

  // SoQLTypes represented by more than one physical columns
  private val complexTypes: Set[SoQLType] = Set(SoQLLocation, SoQLPhone, SoQLUrl)
}
