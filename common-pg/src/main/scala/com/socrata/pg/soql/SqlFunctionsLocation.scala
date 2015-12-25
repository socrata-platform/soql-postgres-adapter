package com.socrata.pg.soql

import java.sql.PreparedStatement

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.soql.SqlFunctions.FunCall
import com.socrata.pg.soql.Sqlizer._
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctions}
import com.socrata.soql.typed.{FunctionCall, StringLiteral}
import com.socrata.soql.types._

import scala.util.parsing.input.NoPosition

trait SqlFunctionsLocation {

  protected def textToLocation(fn: FunCall,
                               rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                               setParams: Seq[SetParam],
                               ctx: Sqlizer.Context,
                               escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(strLit@StringLiteral(value: String, _)) =>
        val location = JsonUtil.parseJson[SoQLLocation](value).right.get
        val sqls = Seq(
          s"ST_GeomFromEWKT('SRID=4326;POINT(${location.longitude.get} ${location.latitude.get})')",
          "?")
        val setParam: SetParam = (stmt: Option[PreparedStatement], pos: Int) => {
          stmt.foreach(_.setString(pos, location.address.getOrElse("")))
          None
        }
        ParametricSql(sqls, setParams :+ setParam)
      case _ => throw new Exception("should never get anything but string literal")
    }
  }

  protected def locationAddress(fn: FunCall,
                              rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                              setParams: Seq[SetParam],
                              ctx: Sqlizer.Context,
                              escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(col) =>
        val ParametricSql(sqls, params) = Sqlizer.sql(col)(rep, setParams, ctx, escape)
        ParametricSql(sqls.drop(1), params) // Drop geom and keep only address
      case _ => throw new Exception("should never get here")
    }
  }

  protected def locationLatLng(prop: String)(fn: FunCall,
                                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                             setParams: Seq[SetParam],
                                             ctx: Sqlizer.Context,
                                             escape: Escape): ParametricSql = {
    val propFn = Map("latitude" -> "ST_Y(%s)::numeric", "longitude" -> "ST_X(%s)::numeric")
    fn.parameters match {
      case  Seq(col) =>
        propFn.get(prop) match {
          case Some(template) =>
            val ParametricSql(sqls, newSetParams) = Sqlizer.sql(col)(rep, setParams, ctx, escape)
            // Take only geom and extract latitude or longitude
            ParametricSql(sqls.take(1).map(template.format(_)), newSetParams)
          case _ =>
            ParametricSql(Seq("null"), setParams)
        }
      case _ => throw new Exception("should never get here")
    }
  }

  protected def locationToPoint(fn: FunCall,
                              rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                              setParams: Seq[SetParam],
                              ctx: Sqlizer.Context,
                              escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(col) =>
        val ParametricSql(sqls, params) = Sqlizer.sql(col)(rep, setParams, ctx, escape)
        ParametricSql(sqls.take(1), params) // Take only geom and drop address
      case _ => throw new Exception("should never get here")
    }
  }

  protected def geometryFunctionWithLocation(geomFunction: com.socrata.soql.functions.Function[SoQLType])
                                            (fn: FunCall,
                                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                                             setParams: Seq[SetParam],
                                             ctx: Sqlizer.Context,
                                             escape: Escape): ParametricSql = {
        val toPointFn = MonomorphicFunction(SoQLFunctions.LocationToPoint, Map.empty)
        val toPointCall = FunctionCall(toPointFn, fn.parameters.take(1))(NoPosition, NoPosition)
        val ParametricSql(sqls, params) = Sqlizer.sql(toPointCall)(rep, setParams, ctx, escape)
        val (bindName, bindType) = fn.function.bindings.head
        val geomFn = MonomorphicFunction(geomFunction, fn.function.bindings - bindName + (bindName -> SoQLPoint))
        val geomParams = toPointCall +: fn.parameters.drop(1)
        val geomCall = FunctionCall(geomFn, geomParams)(NoPosition, NoPosition)
        Sqlizer.sql(geomCall)(rep, setParams, ctx, escape)
  }
}
