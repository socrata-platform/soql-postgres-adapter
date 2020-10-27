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
import com.socrata.pg.soql.SqlizerContext.{RootExpr, SoqlPart}
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
    Least -> nary("least") _,
    Greatest -> nary("greatest") _,
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
    Absolute -> nary("abs") _,
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
    // Extracting the week from a floating timestamp extracts the iso week (1-53), which
    // means that sometimes the last few days of December may be considered the first week
    // of the next year (https://en.wikipedia.org/wiki/ISO_week_date)
    FloatingTimeStampExtractWoy -> formatCall("extract(week from %s)::numeric") _,
  // This is useful when you are also extracting the week (iso week). This is
  // because the iso year will give the year associated with the iso week whereas
  // the year will give the year associated with the iso date.
    FloatingTimestampExtractIsoY -> formatCall("extract(isoyear from %s)::numeric") _,

    FixedTimeStampZTruncYmd -> formatCall("date_trunc('day', %s)") _,
    FixedTimeStampZTruncYm -> formatCall("date_trunc('month', %s)") _,
    FixedTimeStampZTruncY -> formatCall("date_trunc('year', %s)") _,

    FixedTimeStampTruncYmdAtTimeZone -> formatCall("date_trunc('day', %s at time zone %s)") _,
    FixedTimeStampTruncYmAtTimeZone -> formatCall("date_trunc('month', %s at time zone %s)") _,
    FixedTimeStampTruncYAtTimeZone -> formatCall("date_trunc('year', %s at time zone %s)") _,

    TimeStampDiffD -> formatCall("trunc((extract(epoch from %s) - extract(epoch from %s))::numeric / 86400)") _,

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
    TextToPhoto -> passthrough,

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
    Median -> medianContCall _,
    MedianDisc -> formatCall("percentile_disc(.50) within group (order by %s)") _,

    RowNumber -> nary("row_number") _,
    Rank -> nary("rank") _,
    DenseRank -> nary("dense_rank") _,
    FirstValue -> nary("first_value") _,
    LastValue -> nary("last_value") _,

    // TODO: old style - to be deleted
    WindowFunctionOver -> windowOverCall _, //  naryish("over", Some("partition by ")) _,

    Count -> nary("count", Some("numeric")) _,
    CountStar -> formatCall("count(*)", typeCastIfNotWindowFn = Some("numeric")) _,
    CountDistinct -> formatCall("count(distinct %s)", typeCastIfNotWindowFn = Some("numeric")) _
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

  private val coalesceText = {
    val bindings = SoQLFunctions.Coalesce.parameters.map {
      case VariableType(name) => name -> SoQLText.t
      case _ => throw new Exception("Unexpected concat function signature")
    }.toMap
    MonomorphicFunction(SoQLFunctions.Coalesce, bindings)
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
      case FunctionCall(MonomorphicFunction(jsonbFieldKey, _), Seq(field), fn.window) if jsonbFields.contains(jsonbFieldKey) =>
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

  private def nary(fnName: String, returnTypeCast: Option[String] = None)
                  (fn: FunCall,
                   rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                   typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Sqlizer.Context,
                   escape: Escape): ParametricSql = {

    val param2 = specialCountArgument(fn)
    val sqlFragsAndParams = param2.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    val sqlNaryFunctionCall = sqlFragsAndParams._1.mkString(fnName + "(", ",", ")")
    val typeCast = if (fn.window.isDefined) None else returnTypeCast
    val sqlNaryFunctionCallWithTypeCast = typeCast.map(typ => s"($sqlNaryFunctionCall)::$typ" ).getOrElse(sqlNaryFunctionCall)
    ParametricSql(Seq(sqlNaryFunctionCallWithTypeCast), sqlFragsAndParams._2)
  }

  private def specialCountArgument(fn: FunCall): Seq[com.socrata.soql.typed.CoreExpr[UserColumnId, SoQLType]] = {
    fn.function.name match {
      case Count.name =>
        fn.parameters match {
          case Seq(ColumnRef(_, _, typ)) if typ == SoQLUrl.t =>
            // Only if we have chosen jsonb in UrlRep, this would not be necessary
            // only counting url.url - ignoring url.description
            val coalesceArgs = Seq(FunctionCall(UrlToUrl.monomorphic.get, fn.parameters, fn.window)(fn.position, fn.functionNamePosition),
                                   FunctionCall(UrlToDescription.monomorphic.get, fn.parameters, fn.window)(fn.position, fn.functionNamePosition))
            Seq(FunctionCall(coalesceText, coalesceArgs, fn.window)(fn.position, fn.functionNamePosition))
          case _ => fn.parameters
        }
      case _ => fn.parameters
    }
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

  def windowOverInfo(pSql: ParametricSql,
                     fn: FunCall,
                     rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                     typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                     setParams: Seq[SetParam],
                     ctx: Sqlizer.Context,
                     escape: Escape): ParametricSql = {

    fn.window match {
      case Some(windowFunctionInfo) =>
        val sqlPartitions = windowFunctionInfo.partitions.foldLeft(Tuple2(Seq.empty[String], pSql.setParams)) { (acc, param) =>
          val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
          (acc._1 :+ sql, newSetParams)
        }

        val sqlOrderings = windowFunctionInfo.orderings.foldLeft((Seq.empty[String], sqlPartitions._2)) { (acc, ob) =>
          val ParametricSql(Seq(sql), newSetParams) = Sqlizer.sql(ob)(rep, typeRep, acc._2, ctx, escape)
          (acc._1 :+ sql, newSetParams)
        }

        val sqlFrames = windowFunctionInfo.frames.foldLeft((Seq.empty[String], sqlOrderings._2)) { (acc, param) =>
          param match {
            case StringLiteral(x, _) =>
              (acc._1 :+ x, acc._2)
            case NumberLiteral(x, param) =>
              (acc._1 :+ x.toString, acc._2)
            case _ => // should never happen
              acc
          }
        }

        val sqlPartitionsPreamble = if (windowFunctionInfo.partitions.isEmpty) "" else " partition by "
        val sqlOrderingsPreamble = if (windowFunctionInfo.orderings.isEmpty) "" else " order by "
        val sqlFramesPreamble = if (windowFunctionInfo.frames.isEmpty) "" else " "

        val typeCast = fn.function.function match {
          case SoQLFunctions.Count | SoQLFunctions.CountDistinct | SoQLFunctions.CountStar => "::numeric"
          case _ => ""
        }

        val sql = pSql.sql.mkString +
                    " over(" +
                    sqlPartitions._1.mkString(sqlPartitionsPreamble, ",", "") +
                    sqlOrderings._1.mkString(sqlOrderingsPreamble, ",", "") +
                    sqlFrames._1.mkString(sqlFramesPreamble, " ", "") +
                    ")" + typeCast
        ParametricSql(Seq(sql), sqlFrames._2)
      case None =>
        pSql
    }
  }

  private def windowOverCall(fn: FunCall,
                             rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {

    val ctxInsideWindowFn = ctx + (SqlizerContext.NoWrappingParenInFunctionCall -> true) + (SqlizerContext.InsideWindowFn -> true)
    val ParametricSql(Seq(head), setParamsHead) = Sqlizer.sql(fn.parameters.head)(rep, typeRep, setParams, ctxInsideWindowFn, escape)

    var prefixComma = false
    val (sqls: Seq[String], params: Seq[SetParam]) = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(sqls, p) = param match {
        case strLit@StringLiteral(value: String, _) if value == "partition_by" || value == "order_by" =>
          prefixComma = false
          ParametricSql(Seq(" " + value.replace("_", " ")), acc._2)
        case _ =>
          val prefix = if (prefixComma) "," else " "
          prefixComma = true
          val psql = Sqlizer.sql(param)(rep, typeRep, acc._2, ctx, escape)
          psql.copy(sql = psql.sql.map(prefix + _))
      }
      (acc._1 ++ sqls, p)
    }

    val sql = sqls.mkString(head + " over(", "", ")")

    ParametricSql(Seq(sql), params)
  }

  private def medianContCall(fn: FunCall,
                             rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    if (ctx.contains(SqlizerContext.InsideWindowFn)) {
      // percentile_cont doesn't work as a window function so we are using a slightly modified function lifted
      // from https://wiki.postgresql.org/wiki/Aggregate_Median for continuous median.  It can be significantly slower,
      // hosever works as a window function.  We could write a similar one to replace percentile_disc if we had the need,
      // but for now avoiding that effort and testing.
      nary("median_ulib_agg")(fn, rep, typeRep, setParams, ctx, escape)
    } else {
      formatCall("percentile_cont(.50) within group (order by %s)")(fn, rep, typeRep, setParams, ctx, escape)
    }
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
                 paramPosition: Option[Seq[Int]] = None,
                 typeCastIfNotWindowFn: Option[String] = None)
                (fn: FunCall,
                 rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                 typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                 setParams: Seq[SetParam],
                 ctx: Sqlizer.Context,
                 escape: Escape): ParametricSql = {

    val templateTypeCast = if (fn.window.isEmpty && typeCastIfNotWindowFn.isDefined) template + typeCastIfNotWindowFn.map("::" + _).get
                           else template // when window option is used, typecast is done higher up
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
      case None => templateTypeCast.format(sqlFragsAndParams._1:_*)
      case Some(op) =>
        foldSegments(sqlFragsAndParams._1.map(s => templateTypeCast.format(s)), op)
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

    val fnWithConvertedParams = fn.copy(parameters = convertedParams, window = fn.window)
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
    val suffix = FunctionCall(suffixWildcard, params, fn.window)(fn.position, fn.functionNamePosition)
    val wildcardCall =
      if (prefix) { FunctionCall(suffixWildcard, Seq(wildcard, suffix), fn.window)(fn.position, fn.functionNamePosition) }
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
