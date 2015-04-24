package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.functions._
import com.socrata.soql.types._
import com.socrata.soql.typed.{CoreExpr, NumberLiteral, StringLiteral, FunctionCall}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import scala.util.parsing.input.NoPosition
import com.socrata.soql.ast.SpecialFunctions

object SqlFunctions {

  import SoQLFunctions._

  import Sqlizer._

  type FunCall = FunctionCall[UserColumnId, SoQLType]

  type FunCallToSql = (FunCall, Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], Seq[SetParam], Sqlizer.Context, Escape) => ParametricSql

  def apply(function: Function[SoQLType]) = funMap(function)

  private val funMap = Map[Function[SoQLType], FunCallToSql](
    IsNull -> formatCall("%s is null") _,
    IsNotNull -> formatCall("%s is not null") _,
    Not -> formatCall("not %s") _,
    In -> naryish("in") _,
    NotIn -> naryish("not in") _,
    Eq -> infix("=") _,
    EqEq -> infix("=") _,
    Neq -> infix("!=") _,
    BangEq -> infix("!=") _,
    And -> infix("and") _,
    Or -> infix("or") _,
    NotBetween -> formatCall("not %s between %s and %s") _,
    WithinCircle -> formatCall("ST_within(%s, ST_Buffer(ST_MakePoint(%s, %s)::geography, %s)::geometry)", Some(Seq(0, 2, 1, 3))) _,
    WithinPolygon -> formatCall("ST_within(%s, %s)") _,
    // ST_MakeEnvelope(double precision xmin, double precision ymin, double precision xmax, double precision ymax, integer srid=unknown)
    // within_box(location_col_identifier, top_left_latitude, top_left_longitude, bottom_right_latitude, bottom_right_longitude)
    WithinBox -> formatCall("ST_MakeEnvelope(%s, %s, %s, %s, 4326) ~ %s", Some(Seq(2, 3, 4, 1, 0))) _,
    Extent -> formatCall("ST_Multi(ST_Extent(%s))") _,
    ConcaveHull -> formatCall("ST_Multi(ST_ConcaveHull(ST_Union(%s), %s))") _,
    ConvexHull -> formatCall("ST_Multi(ST_ConvexHull(ST_Union(%s)))"),
    Intersects -> formatCall("ST_Intersects(%s, %s)") _,
    Between -> formatCall("%s between %s and %s") _,
    Lt -> infix("<") _,
    Lte -> infix("<=") _,
    Gt -> infix(">")_,
    Gte -> infix(">=") _,
    TextToRowIdentifier -> decryptString(SoQLID) _,
    TextToRowVersion -> decryptString(SoQLVersion) _,
    Like -> infix("like") _,
    NotLike -> infix("not like") _,
    StartsWith -> infixSuffixWildcard("like") _,
    Contains -> infix("like") _,  // TODO - Need to add prefix % and suffix % to the 2nd operand.
    Concat -> infix("||") _,

    Lower -> nary("lower") _,
    Upper -> nary("upper") _,

    // Number
    // http://beta.dev.socrata.com/docs/datatypes/numeric.html
    UnaryPlus -> formatCall("%s"),
    UnaryMinus -> formatCall("-%s") _,
    SignedMagnitude10 -> formatCall("sign(%s) * length(floor(abs(%s))::text)", Some(Seq(0,0))),
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

    FloatingTimeStampTruncYmd -> formatCall("date_trunc('day', %s)") _,
    FloatingTimeStampTruncYm -> formatCall("date_trunc('month', %s)") _,
    FloatingTimeStampTruncY -> formatCall("date_trunc('year', %s)") _,

    // datatype conversions
    // http://beta.dev.socrata.com/docs/datatypes/converting.html
    NumberToText -> formatCall("%s::varchar") _,
    NumberToMoney -> formatCall("%s::numeric") _,

    TextToNumber -> formatCall("%s::numeric") _,
    TextToFixedTimestamp -> formatCall("%s::timestamp with time zone") _,
    TextToFloatingTimestamp -> formatCall("%s::timestamp") _, // without time zone
    TextToMoney -> formatCall("%s::numeric") _,

    TextToBool -> formatCall("%s::boolean") _,
    BoolToText -> formatCall("%s::varchar") _,

    TextToPoint -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiLine -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiPolygon -> formatCall("ST_GeomFromText(%s, 4326)") _,

    // aggregate functions
    Avg -> nary("avg") _,
    Min -> nary("min") _,
    Max -> nary("max") _,
    Sum -> nary("sum") _,
    Count -> nary("count") _,
    CountStar -> formatCall("count(*)") _
    // TODO: Complete the function list.
  ) ++ castIdentities.map(castIdentity => Tuple2(castIdentity, formatCall("%s") _))


  private val Wildcard = StringLiteral("%", SoQLText)(NoPosition)

  private val SuffixWildcard = {
    val bindings = SoQLFunctions.Concat.parameters.map {
      case VariableType(name) =>  (name -> SoQLText)
      case _ => throw new Exception("Unexpected concat function signature")
    }.toMap
    MonomorphicFunction(SoQLFunctions.Concat, bindings)
  }

  private def infix(fnName: String)
                   (fn: FunCall,
                    rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                    setParams: Seq[SetParam],
                    ctx: Sqlizer.Context,
                    escape: Escape): ParametricSql = {
    val ParametricSql(l, setParamsL) = fn.parameters(0).sql(rep, setParams, ctx, escape)
    val ParametricSql(r, setParamsLR) = fn.parameters(1).sql(rep, setParamsL, ctx, escape)
    val s = s"$l $fnName $r"
    ParametricSql(s, setParamsLR)
  }

  private def nary(fnName: String)
                  (fn: FunCall,
                   rep: Map[UserColumnId,
                   SqlColumnRep[SoQLType, SoQLValue]],
                   setParams: Seq[SetParam],
                   ctx: Sqlizer.Context,
                   escape: Escape): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def naryish(fnName: String)
                     (fn: FunCall,
                      rep: Map[UserColumnId,
                      SqlColumnRep[SoQLType, SoQLValue]],
                      setParams: Seq[SetParam],
                      ctx: Sqlizer.Context,
                      escape: Escape): ParametricSql = {

    val ParametricSql(head, setParamsHead) = fn.parameters.head.sql(rep, setParams, ctx, escape)

    val sqlFragsAndParams = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(head + " " + fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def formatCall(template: String, paramPosition: Option[Seq[Int]] = None)
                        (fn: FunCall,
                         rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
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
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(template.format(sqlFragsAndParams._1:_*), sqlFragsAndParams._2)
  }


  private def decryptToNumLit(typ: SoQLType)(idRep: SoQLIDRep, verRep: SoQLVersionRep, encrypted: StringLiteral[SoQLType]) = {
    typ match {
      case SoQLID =>
        idRep.unapply(encrypted.value) match {
          case Some(SoQLID(num)) => NumberLiteral(num, SoQLNumber)(encrypted.position)
          case _ => throw new Exception("Cannot decrypt id")
        }
      case SoQLVersion =>
        verRep.unapply(encrypted.value) match {
          case Some(SoQLVersion(num)) => NumberLiteral(num, SoQLNumber)(encrypted.position)
          case _ => throw new Exception("Cannot decrypt version")
        }
      case _ =>
        throw new Exception("Internal error")
    }
  }

  private def decryptString(typ: SoQLType)
                           (fn: FunCall,
                            rep: Map[UserColumnId,
                            SqlColumnRep[SoQLType, SoQLValue]],
                            setParams: Seq[SetParam],
                            ctx: Sqlizer.Context,
                            escape: Escape): ParametricSql = {
    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      param match {
        case strLit@StringLiteral(value: String, _) =>
          val idRep = ctx(SqlizerContext.IdRep).asInstanceOf[SoQLIDRep]
          val verRep = ctx(SqlizerContext.VerRep).asInstanceOf[SoQLVersionRep]
          val numLit = decryptToNumLit(typ)(idRep, verRep, strLit)
          val ParametricSql(sql, newSetParams) = numLit.sql(rep, acc._2, ctx, escape)
          (acc._1 :+ sql, newSetParams)
        case unexpected =>
          throw new Exception("Row id is not string literal")
      }
    }
    ParametricSql(sqlFragsAndParams._1.mkString(","), sqlFragsAndParams._2)
  }

  private def infixSuffixWildcard(fnName: String)
                                 (fn: FunCall,
                                  rep: Map[UserColumnId,
                                  SqlColumnRep[SoQLType, SoQLValue]],
                                  setParams: Seq[SetParam],
                                  ctx: Sqlizer.Context,
                                  escape: Escape): ParametricSql = {

    val ParametricSql(l, setParamsL) = fn.parameters(0).sql(rep, setParams, ctx, escape)
    val params = Seq(fn.parameters(1), Wildcard)
    val suffixWildcard = FunctionCall(SuffixWildcard, params)(fn.position, fn.functionNamePosition)
    val ParametricSql(r, setParamsLR) = suffixWildcard.sql(rep, setParamsL, ctx, escape)
    val s = s"$l $fnName $r"
    ParametricSql(s, setParamsLR)
  }
}
