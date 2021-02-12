package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.soql.environment.TableName
import com.socrata.soql.exceptions.BadParse
import java.sql.PreparedStatement

import Sqlizer._
import SqlizerContext._
import com.socrata.pg.soql.SqlFunctions.{FunCall, windowOverInfo}


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

  type SetStmtParam = (PreparedStatement, Integer, NumberLiteral[SoQLType]) => Unit

  def sql(lit: NumberLiteral[SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql =
    setSqlStmtParam(setBigDecimal)(lit)(rep, typeRep, setParams, ctx, escape)

  /**
    * For row id/version where the underlying db type is long so that the proper index can be used.
    */
  def sqlUsingLong(lit: NumberLiteral[SoQLType])
                  (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Context,
                   escape: Escape): ParametricSql = {
    setSqlStmtParam(setLong)(lit)(rep, typeRep, setParams, ctx, escape)
  }

  private def setBigDecimal(pStmt: PreparedStatement, pos: Integer, lit: NumberLiteral[SoQLType]): Unit =
    pStmt.setBigDecimal(pos, lit.value.bigDecimal)

  private def setLong(pStmt: PreparedStatement, pos: Integer, lit: NumberLiteral[SoQLType]): Unit =
    pStmt.setLong(pos, lit.value.longValue)

  private def setSqlStmtParam(setStmtParam: SetStmtParam)
                             (lit: NumberLiteral[SoQLType])
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
          stmt.foreach(setStmtParam(_, pos, lit))
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

  private def windowFnCtx(fn: FunCall, ctx: Sqlizer.Context): Sqlizer.Context = {
    if (fn.window.nonEmpty) ctx + (SqlizerContext.NoWrappingParenInFunctionCall -> true) + (SqlizerContext.InsideWindowFn -> true)
    else ctx
  }

  def sql(expr: FunctionCall[UserColumnId, SoQLType])
         (rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val fn = SqlFunctions(expr.function.function)
    val pSql = fn(expr, rep, typeRep, setParams, windowFnCtx(expr, ctx), escape)
    // SoQL parsing bakes parenthesis into the ast tree without explicitly spitting out parenthesis.
    // We add parenthesis to every function call to preserve semantics.
    val ParametricSql(sqls, fnSetParams) = windowOverInfo(pSql, expr, rep, typeRep, pSql.setParams, ctx, escape)
    if (ctx.keySet.contains(NoWrappingParenInFunctionCall)) {
      ParametricSql(sqls.map(s => s"$s" + selectAlias(expr)(ctx)), fnSetParams)
    } else {
      ParametricSql(sqls.map(s => s"($s)" + selectAlias(expr)(ctx)), fnSetParams)
    }
  }
}

object ColumnRefSqlizer extends Sqlizer[ColumnRef[UserColumnId, SoQLType]] {

  private def idQuote(s: String) = s""""$s"""" //   "\"" + s + "\""

  private val qualifierRx = "^[_A-Za-z]+[_A-Za-z0-9]*$".r

  private def qualifierFromAlias(expr: ColumnRef[UserColumnId, SoQLType], aliases: Map[String, String]): Qualifier = {
    expr.qualifier.map(aliases.get(_)).getOrElse(expr.qualifier)
  }

  private def isTableAlias(q: Qualifier, aliases: Map[String, String]): Boolean = {
    q match {
      case Some(x) =>
        aliases.get(x) match {
          case Some(a) => a != x
          case None => false
        }
      case None => false
    }
  }

  // scalastyle:off cyclomatic.complexity
  def sql(expr: ColumnRef[UserColumnId, SoQLType])
         (reps: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
          typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
          setParams: Seq[SetParam],
          ctx: Context,
          escape: Escape): ParametricSql = {
    val tableMap = ctx(TableAliasMap).asInstanceOf[Map[String, String]]

    val simpleJoinMap = ctx(SimpleJoinMap).asInstanceOf[Map[String, String]]

    val useTypeRep = expr.qualifier match {
      case Some(qual) =>
        !simpleJoinMap.contains(qual)
      case None =>
        val chained = ctx(InnermostSoql) != true
        chained
    }

    val ista = isTableAlias(expr.qualifier, simpleJoinMap)

    reps.get(QualifiedUserColumnId(expr.qualifier, expr.column)) match {
      case Some(rep) if !useTypeRep =>
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
          val qualifier = expr.qualifier match {
            case Some(x) =>
              if (ista) expr.qualifier
              else Some(tableMap(x))
            case None =>
              tableMap.get(TableName.PrimaryTable.qualifier)
          }
          qualifier.foreach(qualifierRx.findFirstMatchIn(_).orElse(throw BadParse("Invalid table alias", expr.position)))
          val maybeUpperPhysColumns = rep.physColumns.map(c => toUpper(expr)(qualifier.map(q => s"$q.$c").getOrElse(c), ctx))
          ParametricSql(maybeUpperPhysColumns.map(c => c + selectAlias(expr)(ctx)), setParams)
        }
      case _ =>
        val typeReps = typeRep ++ // TODO: Adding reps seems unnecessary but keep to minimize change for now
          reps.values.map((rep: SqlColumnRep[SoQLType, SoQLValue]) => (rep.representedType -> rep)).toMap
        typeReps.get(expr.typ) match {
          case Some(rep) =>
            val subColumns = rep.physColumns.map { pc => pc.replace(rep.base, "") }
            val sqls = subColumns.map { subCol =>
              val c = idQuote(expr.column.underlying + subCol)
              expr.qualifier.foreach(qualifierRx.findFirstMatchIn(_).orElse(throw BadParse("Invalid table alias", expr.position)))
              val qualifier = expr.qualifier.orElse(ctx.get(PrimaryTableAlias).map(_.toString))
              toUpper(expr)(qualifier.map(q => s"$q.$c").getOrElse(c), ctx) + selectAlias(expr, Some(subCol))(ctx)
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
