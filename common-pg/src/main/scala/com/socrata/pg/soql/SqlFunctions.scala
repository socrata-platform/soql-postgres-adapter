package com.socrata.pg.soql

import scala.util.parsing.input.NoPosition
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.functions._
import com.socrata.soql.typed._
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.soql.types._
import Sqlizer._
import SoQLFunctions._
import com.socrata.soql.exceptions.BadParse
import org.joda.time.{DateTime, LocalDateTime}

// scalastyle:off magic.number multiple.string.literals
object SqlFunctions extends SqlFunctionsLocation with SqlFunctionsGeometry with SqlFunctionsComplexType {
  type FunCall = FunctionCall[UserColumnId, SoQLType]

  type FunCallToSql =
    (FunCall,
     Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
     Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
     Seq[SetParam],
     Sqlizer.Context,
     Escape) => ParametricSql

  val SqlNull = "null"
  val SqlNullInParen = "(null)"
  val SqlParamPlaceHolder = "?"
  val SqlEq = "="
  val SqlNeq = "!="

  def apply(function: Function[SoQLType]): FunCallToSql = funMap(function)

  private val funMap = Map[Function[SoQLType], FunCallToSql](
    IsNull -> formatCall("%s is null", Some(" and ")) _,
    IsNotNull -> formatCall("%s is not null" , Some(" or ")) _,
    Not -> formatCall("not %s") _,
    In -> naryish("in") _,
    NotIn -> naryish("not in") _,
    Eq -> infixWithJsonbEqOptimization(SqlEq) _,
    EqEq -> infixWithJsonbEqOptimization(SqlEq) _,
    Neq -> infix(SqlNeq, " or ") _,
    BangEq -> infix(SqlNeq, " or ") _,
    And -> infix("and") _,
    Or -> infix("or", " or ") _,
    NotBetween -> formatCall("not %s between %s and %s") _,
    Between -> formatCall("%s between %s and %s") _,
    Lt -> infix("<") _,
    Lte -> infix("<=") _,
    Gt -> infix(">")_,
    Gte -> infix(">=") _,
    TextToRowIdentifier -> decryptString(SoQLID) _,
    TextToRowVersion -> decryptString(SoQLVersion) _,
    Like -> infix("like") _,
    NotLike -> infix("not like") _,
    StartsWith -> infixSuffixWildcard("like", prefix = false) _,
    Contains -> infixSuffixWildcard("like", prefix = true) _,
    Concat -> infix("||") _,

    Lower -> nary("lower") _,
    Upper -> nary("upper") _,

    // Number
    // http://beta.dev.socrata.com/docs/datatypes/numeric.html
    UnaryPlus -> passthrough,
    UnaryMinus -> formatCall("-%s") _,
    SignedMagnitude10 -> formatCall(
      "sign(%s) * length(floor(abs(%s))::text)",
      paramPosition = Some(Seq(0,0))),
    SignedMagnitudeLinear ->
      formatCall(
        "case when %s = 1 then floor(%s) else sign(%s) * floor(abs(%s)/%s + 1) end",
        paramPosition = Some(Seq(1,0,0,0,1))),
    BinaryPlus -> infix("+") _,
    BinaryMinus -> infix("-") _,
    TimesNumNum -> infix("*") _,
    TimesDoubleDouble -> infix("*") _,
    TimesNumMoney -> infix("*") _,
    TimesMoneyNum -> infix("*") _,
    DivNumNum -> infix("/") _,
    DivDoubleDouble -> infix("/") _,
    DivMoneyNum -> infix("/") _,
    DivMoneyMoney -> infix("/") _,
    ExpNumNum -> infix("^") _,
    ExpDoubleDouble -> infix("^") _,
    ModNumNum -> infix("%") _,
    ModDoubleDouble -> infix("%") _,
    ModMoneyNum -> infix("%") _,
    ModMoneyMoney -> infix("%") _,
    Ceiling -> nary("ceil") _,
    Floor -> nary("floor") _,

    FloatingTimeStampTruncYmd -> formatCall("date_trunc('day', %s)") _,
    FloatingTimeStampTruncYm -> formatCall("date_trunc('month', %s)") _,
    FloatingTimeStampTruncY -> formatCall("date_trunc('year', %s)") _,

    FloatingTimeStampExtractY-> formatCall("extract(year from %s)::numeric") _,
    FloatingTimeStampExtractM-> formatCall("extract(month from %s)::numeric") _,
    FloatingTimeStampExtractD -> formatCall("extract(day from %s)::numeric") _,
    FloatingTimeStampExtractHh -> formatCall("extract(hour from %s)::numeric") _,
    FloatingTimeStampExtractMm -> formatCall("extract(minute from %s)::numeric") _,
    FloatingTimeStampExtractSs -> formatCall("extract(second from %s)::numeric") _,
    FloatingTimeStampExtractDow -> formatCall("extract(dow from %s)::numeric") _,
    FloatingTimeStampExtractWoy -> formatCall("extract(week from %s)::numeric") _,

    FixedTimeStampTruncYmd -> formatCall("date_trunc('day', %s)") _,
    FixedTimeStampTruncYm -> formatCall("date_trunc('month', %s)") _,
    FixedTimeStampTruncY -> formatCall("date_trunc('year', %s)") _,

    FixedTimeStampTruncYmdAtTimeZone -> formatCall("date_trunc('day', %s at time zone %s)") _,
    FixedTimeStampTruncYmAtTimeZone -> formatCall("date_trunc('month', %s at time zone %s)") _,
    FixedTimeStampTruncYAtTimeZone -> formatCall("date_trunc('year', %s at time zone %s)") _,

    // Translate a fixed timestamp to a given time zone and convert it to a floating timestamp.
    ToFloatingTimestamp -> formatCall("%s at time zone %s") _,

    // datatype conversions
    // http://beta.dev.socrata.com/docs/datatypes/converting.html
    NumberToText -> formatCall("%s::varchar") _,
    NumberToMoney -> passthrough,
    NumberToDouble -> formatCall("%s::float") _,

    TextToNumber -> formatCall("%s::numeric") _,
    TextToFixedTimestamp -> textToType("%s::timestamp with time zone",
                                       ((SoQLFixedTimestamp.StringRep.unapply _): String => Option[DateTime]) andThen
                                       ((x: Option[DateTime]) => x.get) andThen
                                       SoQLFixedTimestamp.StringRep.apply) _,
    TextToFloatingTimestamp ->
      textToType("%s::timestamp", // without time zone
                 ((SoQLFloatingTimestamp.StringRep.unapply _): String => Option[LocalDateTime]) andThen
                 ((x: Option[LocalDateTime]) => x.get) andThen
                 SoQLFloatingTimestamp.StringRep.apply) _,
    TextToMoney -> formatCall("%s::numeric") _,
    TextToBlob -> passthrough,

    TextToBool -> formatCall("%s::boolean") _,
    BoolToText -> formatCall("%s::varchar") _,

    Case -> caseCall _,
    Coalesce -> coalesceCall _,

    // aggregate functions
    Avg -> nary("avg") _,
    Min -> nary("min") _,
    Max -> nary("max") _,
    Sum -> nary("sum") _,
    StddevPop -> nary("stddev_pop") _,
    StddevSamp -> nary("stddev_samp") _,
    Median -> formatCall("percentile_disc(.50) within group (order by %s)") _,

    WindowFunctionOver -> naryish("over", Some("partition by ")) _,

    Count -> nary("count") _,
    CountStar -> formatCall("count(*)") _
    // TODO: Complete the function list.
  ) ++
    funGeometryMap ++
    funLocationMap ++
    funComplexTypeMap ++
    castIdentities.map(castIdentity => Tuple2(castIdentity, passthrough))

  private val wildcard = StringLiteral("%", SoQLText.t)(NoPosition)

  private val suffixWildcard = {
    val bindings = SoQLFunctions.Concat.parameters.map {
      case VariableType(name) => name -> SoQLText.t
      case _ => throw new Exception("Unexpected concat function signature")
    }.toMap
    MonomorphicFunction(SoQLFunctions.Concat, bindings)
  }

  private def passthrough: FunCallToSql = formatCall("%s")

  private def infix(fnName: String, foldOp: String = " and ")
                   (fn: FunCall,
                    rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                    typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                    setParams: Seq[SetParam],
                    ctx: Sqlizer.Context,
                    escape: Escape): ParametricSql = {
    val ParametricSql(ls, setParamsL) = Sqlizer.sql(fn.parameters(0))(rep, typeRep, setParams, ctx, escape)
    val ParametricSql(rs, setParamsLR) = Sqlizer.sql(fn.parameters(1))(rep, typeRep, setParamsL, ctx, escape)

    val moreThanOneExpression = ls.tail.nonEmpty
    val lrs = ls.zip(rs).foldLeft(Seq.empty[String]) { (acc, lr) =>
      lr match {
        case (_, r) if moreThanOneExpression && r == SqlNullInParen =>
          // Choose to not compare null to make it easy to work with obe.
          // May be consider skipping nulls only by flag in the future.
          // select phone, phone_type where phone = '4251234567' is rewritten by fuse column to
          // select phone(phone, phone_type) where phone(phone, phone_type) = '4251234567'
          // ignore the null field in compound type.
          acc
        case (l, r) =>
          val s = if (fnName == SqlEq && r == SqlNullInParen) { s"$l is null" }
                  else if (fnName == SqlNeq && r == SqlNullInParen) { s"$l is not null" }
                  else { s"$l $fnName $r" }
          acc :+ s
      }
    }

    val s = foldSegments(lrs, foldOp)
    ParametricSql(Seq(s), setParamsLR)
  }

  private val jsonbFields = Map(DocumentToFilename -> "filename",
                                DocumentToFileId -> "file_id",
                                DocumentToContentType -> "content_type")

  /**
    * Detect jsonb optimization.  If there is none, call infix.
    */
  private def infixWithJsonbEqOptimization(fnName: String, foldOp: String = " and ")
                                          (fn: FunCall,
                                           rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                           typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                           setParams: Seq[SetParam],
                                           ctx: Sqlizer.Context,
                                           escape: Escape): ParametricSql = {
    val optimized = fn.parameters(0) match {
      case FunctionCall(MonomorphicFunction(jsonbFieldKey, _), Seq(field)) if jsonbFields.contains(jsonbFieldKey) =>
        fn.parameters(1) match {
          case strLit@StringLiteral(value: String, _) =>
            val ParametricSql(ls, setParamsL) = Sqlizer.sql(field)(rep, typeRep, setParams, ctx, escape)
            val ParametricSql(rs, setParamsLR) = Sqlizer.sql(fn.parameters(1))(rep, typeRep, setParamsL, ctx, escape)
            val lrs = ls.zip(rs).foldLeft(Seq.empty[String]) { (acc, lr) =>
              lr match {
                case (l, r) =>
                  val fieldName = jsonbFields(jsonbFieldKey)
                  val s = { s"""$l @> ('{"$fieldName":"' || ? || '"}')::jsonb""" }
                  acc :+ s
              }
            }
            Some(ParametricSql(lrs, setParamsLR))
          case _ =>
            None
        }
      case _ =>
        None
    }

    optimized match {
      case Some(x) => x
      case None =>
        infix(fnName, foldOp)(fn, rep, typeRep, setParams, ctx, escape)
    }
  }

  private def nary(fnName: String)
                  (fn: FunCall,
                   rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Sqlizer.Context,
                   escape: Escape): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(Seq(sqlFragsAndParams._1.mkString(fnName + "(", ",", ")")), sqlFragsAndParams._2)
  }

  private def naryish(fnName: String, afterOpenParenText: Option[String] = None)
                     (fn: FunCall,
                      rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                      typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                      setParams: Seq[SetParam],
                      ctx: Sqlizer.Context,
                      escape: Escape): ParametricSql = {

    val ParametricSql(Seq(head), setParamsHead) = Sqlizer.sql(fn.parameters.head)(rep, typeRep, setParams, ctx + (SqlizerContext.NoWrappingParenInFunctionCall -> true), escape)

    val sqlFragsAndParams = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    val aopt = afterOpenParenText match {
      case Some(x) if sqlFragsAndParams._1.nonEmpty => x
      case _ => ""
    }
    ParametricSql(Seq(sqlFragsAndParams._1.mkString(head + " " + fnName + "(" + aopt, ",", ")")), sqlFragsAndParams._2)
  }

  private def caseCall(fn: FunCall,
                       rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                       typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                       setParams: Seq[SetParam],
                       ctx: Sqlizer.Context,
                       escape: Escape): ParametricSql = {
    val whenThens = fn.parameters.toSeq.grouped(2) // make each when, then expressions into a pair (seq)
    val (sqls, params) = whenThens.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      param match {
        case Seq(when, thenDo) =>
          val ParametricSql(Seq(whenSql), whenSetParams) = Sqlizer.sql(when)(rep, typeRep, acc._2, ctx, escape)
          val ParametricSql(Seq(thenSql), thenSetParams) = Sqlizer.sql(thenDo)(rep, typeRep, whenSetParams, ctx, escape)
          (acc._1 :+ s"WHEN $whenSql" :+ s"THEN $thenSql", thenSetParams)
        case _ => throw new Exception("invalid case statement")
      }
    }

    val caseSql = sqls.mkString("case ", " ", " end")
    ParametricSql(Seq(caseSql), params)
  }

  private def coalesceCall(fn: FunCall,
                           rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                           typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                           setParams: Seq[SetParam],
                           ctx: Sqlizer.Context,
                           escape: Escape): ParametricSql = {
    val (sqls, params) = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sqls, p) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
      (acc._1 ++ sqls, p)
    }
    val sql = sqls.mkString("coalesce(", ",", ")")
    ParametricSql(Seq(sql), params)
  }

  /**
   * Fold sql segments into one for datatypes that have multiple pg columns.
   * SoQLLocation is the only type.  Multiple pg columns type is not something
   * that we would normally like to use except for compatibility with legacy types.
   */
  private def foldSegments(sqls: Seq[String], foldOp: String): String = {
    sqls.mkString(foldOp)
  }

  def notAvailable(fn: FunCall,
                   rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Sqlizer.Context,
                   escape: Escape): ParametricSql = {
    throw BadParse(s"${fn.function.name.name} is not available", fn.functionNamePosition)
  }

  def formatCall(template: String, // scalastyle:ignore parameter.number
                 foldOp: Option[String] = None,
                 paramPosition: Option[Seq[Int]] = None)
                (fn: FunCall,
                 rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                 typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                 setParams: Seq[SetParam],
                 ctx: Sqlizer.Context,
                 escape: Escape): ParametricSql = {

    val fnParams = paramPosition match {
      case Some(pos) =>
        pos.foldLeft(Seq.empty[CoreExpr[UserColumnId, SoQLType]]) { (acc, param) =>
          acc :+ fn.parameters(param)
        }
      case None => fn.parameters
    }

    val sqlFragsAndParams = fnParams.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sqls, newSetParams) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
      (acc._1 ++ sqls, newSetParams)
    }

    val foldedSql = foldOp match {
      case None => template.format(sqlFragsAndParams._1:_*)
      case Some(op) =>
        foldSegments(sqlFragsAndParams._1.map(s => template.format(s)), op)
    }
    ParametricSql(foldedSql.split(SqlFragments.SeparatorRx).toSeq, sqlFragsAndParams._2)
  }

  private def decryptToNumLit(typ: SoQLType)(idRep: SoQLIDRep,
                                             verRep: SoQLVersionRep,
                                             encrypted: StringLiteral[SoQLType]) = {
    typ match {
      case SoQLID =>
        idRep.unapply(encrypted.value) match {
          case Some(SoQLID(num)) => NumberLiteral[SoQLType](num, SoQLNumber)(encrypted.position)
          case _ => throw new Exception("Cannot decrypt id")
        }
      case SoQLVersion =>
        verRep.unapply(encrypted.value) match {
          case Some(SoQLVersion(num)) => NumberLiteral[SoQLType](num, SoQLNumber)(encrypted.position)
          case _ => throw new Exception("Cannot decrypt version")
        }
      case _ => throw new Exception("Internal error")
    }
  }

  private def decryptString(typ: SoQLType)
                           (fn: FunCall,
                            rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                            typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                            setParams: Seq[SetParam],
                            ctx: Sqlizer.Context,
                            escape: Escape): ParametricSql = {
    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      param match {
        case strLit@StringLiteral(value: String, _) =>
          val idRep = ctx(SqlizerContext.IdRep).asInstanceOf[SoQLIDRep]
          val verRep = ctx(SqlizerContext.VerRep).asInstanceOf[SoQLVersionRep]
          val numLit = decryptToNumLit(typ)(idRep, verRep, strLit)
          val ParametricSql(Seq(sql), newSetParams) = NumberLiteralSqlizer.sqlUsingLong(numLit)(rep, typeRep, acc._2, ctx, escape)
          (acc._1 :+ sql, newSetParams)
        case _ => throw new Exception("Row id is not string literal")
      }
    }
    ParametricSql(Seq(sqlFragsAndParams._1.mkString(",")), sqlFragsAndParams._2)
  }

  private def textToType(template: String, conversion: String => String)
                        (fn: FunCall,
                         rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                         typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                         setParams: Seq[SetParam],
                         ctx: Sqlizer.Context,
                         escape: Escape): ParametricSql = {
    val convertedParams = fn.parameters.map {
      case strLit@StringLiteral(value: String, _) => strLit.copy(value = conversion(value))
      case x => x
    }

    val fnWithConvertedParams = fn.copy(parameters = convertedParams)
    formatCall(template, None, None)(fnWithConvertedParams, rep, typeRep, setParams, ctx, escape)
  }

  // scalastyle:off parameter.number
  private def infixSuffixWildcard(fnName: String, prefix: Boolean, foldOp: String = " and ")
                                 (fn: FunCall,
                                  rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                  typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                  setParams: Seq[SetParam],
                                  ctx: Sqlizer.Context,
                                  escape: Escape): ParametricSql = {
    val ParametricSql(ls, setParamsL) = Sqlizer.sql(fn.parameters(0))(rep, typeRep, setParams, ctx, escape)
    val params = Seq(fn.parameters(1), wildcard)
    val suffix = FunctionCall(suffixWildcard, params)(fn.position, fn.functionNamePosition)
    val wildcardCall =
      if (prefix) { FunctionCall(suffixWildcard, Seq(wildcard, suffix))(fn.position, fn.functionNamePosition) }
      else { suffix }
    val ParametricSql(rs, setParamsLR) = Sqlizer.sql(wildcardCall)(rep, typeRep, setParamsL, ctx, escape)
    val lrs = ls.zip(rs).map { case (l, r) => s"$l $fnName $r" }
    val foldedSql = foldSegments(lrs, foldOp)
    ParametricSql(Seq(foldedSql), setParamsLR)
  }
  // scalastyle:on parameter.number
}

object SqlFragments {
  val Separator = "\n\n"
  val SeparatorRx = "\\n\\n"
}
