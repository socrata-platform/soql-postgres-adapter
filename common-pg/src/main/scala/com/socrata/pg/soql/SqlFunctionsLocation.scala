package com.socrata.pg.soql

import java.sql.PreparedStatement

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.soql.Sqlizer._
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.functions.{Function, MonomorphicFunction, SoQLFunctions}
import com.socrata.soql.typed.{FunctionCall, StringLiteral}
import com.socrata.soql.types._

import scala.util.parsing.input.NoPosition

import com.socrata.pg.soql.SqlFunctions._

trait SqlFunctionsLocation {

  protected val funLocationMap = Map[Function[SoQLType], FunCallToSql](
    TextToLocation -> textToLocation _,
    LocationToPoint -> locationToPoint _,
    LocationToLatitude -> locationLatLng("latitude"),
    LocationToLongitude -> locationLatLng("longitude"),
    LocationToAddress -> locationAddress _,
    // The line break in the verbatium string is significant which keep the two sub-fields as
    // two separate fields like (f1), (f2) instead of (f1, f2)
    // TODO: May be we should not wrap expression in () to begin with.
    Location -> formatCall("%s" + SqlFragments.Separator + humanAddress) _,
    HumanAddress -> formatCall(humanAddress) _,
    LocationWithinCircle -> geometryFunctionWithLocation(SoQLFunctions.WithinCircle),
    LocationWithinBox -> geometryFunctionWithLocation(SoQLFunctions.WithinBox)
  )

  /**
   * TODO: human_address will be null if any address field is null.  But we cannot do a simple coalesce
   * TODO: without handling the edge case to keep human_address as null when all address fields are indeed null.
   */
  private def humanAddress: String = {
    Seq("address", "city", "state", "zip")
      .map(f => s""""$f": ' || to_json(%s::text)::text || '""") // to_json::text encodes special chars in json string.
      .mkString("'{", ", ", "}'")
  }

  private def textToLocation(fn: FunCall,
                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(strLit@StringLiteral(value: String, _)) =>
        val location = JsonUtil.parseJson[SoQLLocation](value).right.get
        val sqls = Seq(
          ( for {
              lat <- location.latitude
              lng <- location.longitude
            } yield {
              s"ST_GeomFromEWKT('SRID=4326;POINT($lng $lat)')"
            }
          ).getOrElse(SqlNull),
          (if (location.address.isDefined) { SqlParamPlaceHolder } else { SqlNull } ))
        val setParam: Seq[SetParam] = location.address.map { addr =>
          (stmt: Option[PreparedStatement], pos: Int) => { stmt.foreach(_.setString(pos, addr))
            Some(addr)
          }}.toSeq
        ParametricSql(sqls, setParams ++ setParam)
      case _ => throw new Exception("should never get anything but string literal")
    }
  }

  private def locationAddress(fn: FunCall,
                              rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                              typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                              setParams: Seq[SetParam],
                              ctx: Sqlizer.Context,
                              escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(col) =>
        val ParametricSql(sqls, params) = Sqlizer.sql(col)(rep, typeRep, setParams, ctx, escape)
        ParametricSql(sqls.drop(1), params) // Drop geom and keep only address
      case _ => throw new Exception("should never get here")
    }
  }

  private def locationLatLng(prop: String)(fn: FunCall,
                                           rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                           typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                           setParams: Seq[SetParam],
                                           ctx: Sqlizer.Context,
                                           escape: Escape): ParametricSql = {
    val propFn = Map("latitude" -> "ST_Y(%s)::numeric", "longitude" -> "ST_X(%s)::numeric")
    fn.parameters match {
      case  Seq(col) =>
        propFn.get(prop) match {
          case Some(template) =>
            val ParametricSql(sqls, newSetParams) = Sqlizer.sql(col)(rep, typeRep, setParams, ctx, escape)
            // Take only geom and extract latitude or longitude
            ParametricSql(sqls.take(1).map(template.format(_)), newSetParams)
          case _ =>
            ParametricSql(Seq("null"), setParams)
        }
      case _ => throw new Exception("should never get here")
    }
  }

  private def locationToPoint(fn: FunCall,
                              rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                              typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                              setParams: Seq[SetParam],
                              ctx: Sqlizer.Context,
                              escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(col) =>
        val ParametricSql(sqls, params) = Sqlizer.sql(col)(rep, typeRep, setParams, ctx, escape)
        ParametricSql(sqls.take(1), params) // Take only geom and drop address
      case _ => throw new Exception("should never get here")
    }
  }

  private def geometryFunctionWithLocation(geomFunction: com.socrata.soql.functions.Function[SoQLType])
                                            (fn: FunCall,
                                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                                             setParams: Seq[SetParam],
                                             ctx: Sqlizer.Context,
                                             escape: Escape): ParametricSql = {
        val toPointFn = MonomorphicFunction(SoQLFunctions.LocationToPoint, Map.empty)
        val toPointCall = FunctionCall(toPointFn, fn.parameters.take(1))(NoPosition, NoPosition)
        val ParametricSql(sqls, params) = Sqlizer.sql(toPointCall)(rep, typeRep, setParams, ctx, escape)
        val (bindName, bindType) = fn.function.bindings.head
        val geomFn = MonomorphicFunction(geomFunction, Map(bindName -> SoQLPoint, "b" -> bindType))
        val geomParams = toPointCall +: fn.parameters.drop(1)
        val geomCall = FunctionCall(geomFn, geomParams)(NoPosition, NoPosition)
        Sqlizer.sql(geomCall)(rep, typeRep, setParams, ctx, escape)
  }
}
