package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.functions._
import com.socrata.soql.typed.{CoreExpr, FunctionCall, NumberLiteral, StringLiteral}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}
import com.socrata.soql.types._
import Sqlizer._

import scala.util.parsing.input.NoPosition

// scalastyle:off magic.number multiple.string.literals
object SqlFunctions {
  import SoQLFunctions._ // scalastyle:ignore import.grouping

  type FunCall = FunctionCall[UserColumnId, SoQLType]

  type FunCallToSql =
    (FunCall, Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], Seq[SetParam], Sqlizer.Context, Escape)
      => ParametricSql

  def apply(function: Function[SoQLType]): FunCallToSql = funMap(function)

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
    WithinCircle -> formatCall(
      "ST_within(%s, ST_Buffer(ST_MakePoint(%s, %s)::geography, %s)::geometry)", Some(Seq(0, 2, 1, 3))) _,
    WithinPolygon -> formatCall("ST_within(%s, %s)") _,
    // ST_MakeEnvelope(double precision xmin, double precision ymin,
    //   double precision xmax, double precision ymax,
    //   integer srid=unknown)
    // within_box(location_col_identifier,
    //   top_left_latitude, top_left_longitude,
    //   bottom_right_latitude, bottom_right_longitude)
    WithinBox -> formatCall("ST_MakeEnvelope(%s, %s, %s, %s, 4326) ~ %s", Some(Seq(2, 3, 4, 1, 0))) _,
    Extent -> formatCall("ST_Multi(ST_Extent(%s))") _,
    ConcaveHull -> formatCall("ST_Multi(ST_ConcaveHull(ST_Union(%s), %s))") _,
    ConvexHull -> formatCall("ST_Multi(ST_ConvexHull(ST_Union(%s)))"),
    Intersects -> formatCall("ST_Intersects(%s, %s)") _,
    DistanceInMeters -> formatCall("ST_Distance(%s::geography, %s::geography)") _,
    Simplify -> formatSimplify("ST_SimplifyPreserveTopology(%s, %s)") _,
    VisibleAt -> visibleAt,
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
    UnaryPlus -> passthrough,
    UnaryMinus -> formatCall("-%s") _,
    SignedMagnitude10 -> formatCall("sign(%s) * length(floor(abs(%s))::text)", Some(Seq(0,0))),
    SignedMagnitudeLinear -> formatCall("sign(%s) * floor(abs(%s)/%s + 1)", Some(Seq(0,0,1))),
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
    NumberToMoney -> passthrough,

    TextToNumber -> formatCall("%s::numeric") _,
    TextToFixedTimestamp -> formatCall("%s::timestamp with time zone") _,
    TextToFloatingTimestamp -> formatCall("%s::timestamp") _, // without time zone
    TextToMoney -> formatCall("%s::numeric") _,

    TextToBool -> formatCall("%s::boolean") _,
    BoolToText -> formatCall("%s::varchar") _,

    TextToPoint -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiPoint -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToLine -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiLine -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToPolygon -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiPolygon -> formatCall("ST_GeomFromText(%s, 4326)") _,

    CuratedRegionTest -> curatedRegionTest,

    Case -> caseCall _,

    // aggregate functions
    Avg -> nary("avg") _,
    Min -> nary("min") _,
    Max -> nary("max") _,
    Sum -> nary("sum") _,
    Count -> nary("count") _,
    CountStar -> formatCall("count(*)") _
    // TODO: Complete the function list.
  ) ++ castIdentities.map(castIdentity => Tuple2(castIdentity, passthrough))

  private val wildcard = StringLiteral("%", SoQLText)(NoPosition)

  private val suffixWildcard = {
    val bindings = SoQLFunctions.Concat.parameters.map {
      case VariableType(name) => name -> SoQLText
      case _ => throw new Exception("Unexpected concat function signature")
    }.toMap
    MonomorphicFunction(SoQLFunctions.Concat, bindings)
  }

  private def passthrough: FunCallToSql = formatCall("%s")

  private def infix(fnName: String)
                   (fn: FunCall,
                    rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                    setParams: Seq[SetParam],
                    ctx: Sqlizer.Context,
                    escape: Escape): ParametricSql = {
    val ParametricSql(l, setParamsL) = Sqlizer.sql(fn.parameters(0))(rep, setParams, ctx, escape)
    val ParametricSql(r, setParamsLR) = Sqlizer.sql(fn.parameters(1))(rep, setParamsL, ctx, escape)
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
      val ParametricSql(sql, newSetParams) = Sqlizer.sql(param)(rep, acc._2, ctx, escape)
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

    val ParametricSql(head, setParamsHead) = Sqlizer.sql(fn.parameters.head)(rep, setParams, ctx, escape)

    val sqlFragsAndParams = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = Sqlizer.sql(param)(rep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(head + " " + fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def caseCall(fn: FunCall,
                       rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                       setParams: Seq[SetParam],
                       ctx: Sqlizer.Context,
                       escape: Escape): ParametricSql = {
    val whenThens = fn.parameters.toSeq.grouped(2) // make each when, then expressions into a pair (seq)
    val (sqls, params) = whenThens.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      param match {
        case Seq(when, thenDo) =>
          val ParametricSql(whenSql, whenSetParams) = Sqlizer.sql(when)(rep, acc._2, ctx, escape)
          val ParametricSql(thenSql, thenSetParams) = Sqlizer.sql(thenDo)(rep, whenSetParams, ctx, escape)
          (acc._1 :+ s"WHEN $whenSql" :+ s"THEN $thenSql", thenSetParams)
        case _ => throw new Exception("invalid case statement")
      }
    }

    val caseSql = sqls.mkString("case ", " ", " end")
    ParametricSql(caseSql, params)
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
      val ParametricSql(sql, newSetParams) = Sqlizer.sql(param)(rep, acc._2, ctx, escape)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(template.format(sqlFragsAndParams._1:_*), sqlFragsAndParams._2)
  }

  private def formatSimplify(template: String, paramPosition: Option[Seq[Int]] = None)
                            (fn: FunCall,
                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    val result@ParametricSql(sql, params) = formatCall(template, paramPosition)(fn, rep, setParams, ctx, escape)
    fn.parameters.head.typ match {
      case SoQLMultiPolygon | SoQLMultiLine =>
        // Simplify can change multipolygon to polygon.  Add ST_Multi to retain its multi nature.
        ParametricSql("ST_Multi(%s)".format(sql), params)
      case _ =>
        result
    }
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
          val ParametricSql(sql, newSetParams) = Sqlizer.sql(numLit)(rep, acc._2, ctx, escape)
          (acc._1 :+ sql, newSetParams)
        case _ => throw new Exception("Row id is not string literal")
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

    val ParametricSql(l, setParamsL) = Sqlizer.sql(fn.parameters(0))(rep, setParams, ctx, escape)
    val params = Seq(fn.parameters(1), wildcard)
    val suffix = FunctionCall(suffixWildcard, params)(fn.position, fn.functionNamePosition)
    val ParametricSql(r, setParamsLR) = Sqlizer.sql(suffix)(rep, setParamsL, ctx, escape)
    val s = s"$l $fnName $r"
    ParametricSql(s, setParamsLR)
  }

  private def curatedRegionTest = {
    formatCall(
      """case when st_npoints(%s) > %s then 'too complex'
              when st_xmin(%s) < -180 or st_xmax(%s) > 180 or st_ymin(%s) < -90 or st_ymax(%s) > 90 then 'out of bounds'
              when not st_isvalid(%s) then st_isvalidreason(%s)::text
              when (%s) is null then 'empty'
         end
      """.stripMargin,
      Some(Seq(0, 1, 0, 0, 0, 0, 0, 0, 0))) _
  }

  private def visibleAt =
    formatCall(
      """(ST_XMax(%s) - ST_XMin(%s)) >= %s OR (ST_YMax(%s) - ST_YMin(%s)) >= %s""",
      Some(Seq(0, 0, 1, 0, 0, 1))) _
}
