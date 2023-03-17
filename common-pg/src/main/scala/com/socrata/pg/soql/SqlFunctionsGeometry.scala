package com.socrata.pg.soql

import scala.math.{Pi, atan, exp}

import com.vividsolutions.jts.geom.Geometry

import com.socrata.datacoordinator.common.soql.sqlreps.GeometryLikeRep
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.soql.SqlFunctions._
import com.socrata.pg.soql.Sqlizer._
import com.socrata.soql.functions.Function
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed.NumberLiteral
import com.socrata.soql.types.{SoQLPolygon, SoQLMultiLine, SoQLMultiPolygon, SoQLValue, SoQLType}

// scalastyle:off magic.number multiple.string.literals
trait SqlFunctionsGeometry {

  protected val funGeometryMap = Map[Function[SoQLType], FunCallToSql](
    TextToPoint -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiPoint -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToLine -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiLine -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToPolygon -> formatCall("ST_GeomFromText(%s, 4326)") _,
    TextToMultiPolygon -> formatCall("ST_GeomFromText(%s, 4326)") _,

    WithinCircle -> formatCall(
      "ST_within(%s, ST_Buffer(ST_MakePoint(%s, %s)::geography, %s)::geometry)",
      paramPosition = Some(Seq(0, 2, 1, 3))) _,
    WithinPolygon -> formatCall("ST_within(%s, %s)") _,
    // ST_MakeEnvelope(double precision xmin, double precision ymin,
    //   double precision xmax, double precision ymax,
    //   integer srid=unknown)
    // within_box(location_col_identifier,
    //   top_left_latitude, top_left_longitude,
    //   bottom_right_latitude, bottom_right_longitude)
    WithinBox -> formatCall(
      "ST_MakeEnvelope(%s, %s, %s, %s, 4326) ~ %s",
      paramPosition = Some(Seq(2, 3, 4, 1, 0))) _,
    Extent -> formatCall("ST_Multi(ST_Extent(%s))") _,
    Union2Pt -> formatCall("ST_Multi(ST_Union(%s, %s))") _,
    Union2Line -> formatCall("ST_Multi(ST_Union(%s, %s))") _,
    Union2Poly -> formatCall("ST_Multi(ST_Union(%s, %s))") _,
    UnionAggPt -> formatCall("ST_Multi(ST_Union(%s))") _,
    UnionAggLine -> formatCall("ST_Multi(ST_Union(%s))") _,
    UnionAggPoly -> formatCall("ST_Multi(ST_Union(%s))") _,
    ConcaveHull -> formatCall("ST_Multi(ST_Buffer(ST_ConcaveHull(%s, %s),0.0))") _,
    ConvexHull -> formatCall("ST_Multi(ST_Buffer(ST_ConvexHull(%s),0.0))"),
    Intersects -> formatCall("ST_Intersects(%s, %s)") _,
    Crosses -> formatCall("ST_Crosses(%s, %s)") _,
    Overlaps -> formatCall("ST_Overlaps(%s, %s)") _,
    // According to https://postgis.net/docs/ST_Intersection.html the Buffer at 0.0 will ensure
    // that any non-polygon intersections in the GeoCollection return are removed and only
    // the multipolygon intersection is returned
    Intersection -> formatCall("ST_Multi(ST_Buffer(ST_Intersection(%s, %s),0.0))") _,
    Area -> formatCall("ST_Area(%s :: geography)::numeric") _,
    DistanceInMeters -> formatCall("ST_Distance(%s::geography, %s::geography)::numeric") _,
    GeoMakeValid -> formatValidate("ST_MakeValid(%s)") _,
    GeoMultiPolygonFromMultiPolygon -> formatCall("ST_Multi(%s)") _,
    GeoMultiLineFromMultiLine -> formatCall("ST_Multi(%s)") _,
    GeoMultiPointFromMultiPoint -> formatCall("ST_Multi(%s)") _,
    GeoMultiPolygonFromPolygon -> formatCall("ST_Multi(%s)") _,
    GeoMultiLineFromLine -> formatCall("ST_Multi(%s)") _,
    GeoMultiPointFromPoint -> formatCall("ST_Multi(%s)") _,
    // ST_CollectionExtract takes a type as a second argument
    // See https://postgis.net/docs/ST_CollectionExtract.html for exact integer -> type mapping
    GeoCollectionExtractMultiPolygonFromPolygon -> formatCall("ST_CollectionExtract(%s, 3)") _,
    GeoCollectionExtractMultiLineFromLine -> formatCall("ST_CollectionExtract(%s, 2)") _,
    GeoCollectionExtractMultiPointFromPoint -> formatCall("ST_CollectionExtract(%s, 1)") _,
    CuratedRegionTest -> curatedRegionTest,
    NumberOfPoints -> formatCall("ST_NPoints(%s)") _,
    Simplify -> formatSimplify("ST_Simplify(%s, %s)") _,
    SimplifyPreserveTopology -> formatSimplify("ST_SimplifyPreserveTopology(%s, %s)") _,
    SnapToGrid -> formatSimplify("ST_SnapToGrid(%s, %s)") _,
    PointToLatitude -> formatCall("ST_Y(%s)::numeric") _,
    PointToLongitude -> formatCall("ST_X(%s)::numeric") _,
    VisibleAt -> visibleAt,
    IsEmpty -> isEmpty
  )

  private def formatSimplify(template: String, paramPosition: Option[Seq[Int]] = None)
                            (fn: FunCall,
                             rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    val result@ParametricSql(Seq(sql), params) =
      formatCall(template, paramPosition = paramPosition)(fn, rep, typeRep, setParams, ctx, escape)
    fn.parameters.head.typ match {
      case SoQLMultiPolygon | SoQLMultiLine =>
        // Simplify can change multipolygon to polygon.  Add ST_Multi to retain its multi nature.
        ParametricSql(Seq("ST_Multi(%s)".format(sql)), params)
      case _ =>
        result
    }
  }

  private def formatValidate(template: String, paramPosition: Option[Seq[Int]] = None)
                            (fn: FunCall,
                             rep: Map[QualifiedUserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    val result@ParametricSql(Seq(sql), params) =
      formatCall(template, paramPosition = paramPosition)(fn, rep, typeRep, setParams, ctx, escape)
    fn.parameters.head.typ match {
      case SoQLMultiPolygon | SoQLPolygon =>
        // Validate can change a polygon to multipolygon.  Add ST_Multi to make everything multi
        ParametricSql(Seq("ST_Multi(%s)".format(sql)), params)
      case _ =>
        result
    }
  }

  private def isEmpty =
    formatCall("ST_IsEmpty(%s) or %s is null", paramPosition = Some(Seq(0, 0))) _

  private def visibleAt =
    formatCall(
      """(NOT ST_IsEmpty(%s)) AND (ST_GeometryType(%s) = 'ST_Point' OR ST_GeometryType(%s) = 'ST_MultiPoint' OR
         (ST_XMax(%s) - ST_XMin(%s)) >= %s OR (ST_YMax(%s) - ST_YMin(%s)) >= %s)
      """.stripMargin,
      paramPosition = Some(Seq(0, 0, 0, 0, 0, 1, 0, 0, 1))) _


  private def curatedRegionTest = {
    formatCall(
      """case when st_npoints(%s) > %s then 'too complex'
              when st_xmin(%s) < -180 or st_xmax(%s) > 180 or st_ymin(%s) < -90 or st_ymax(%s) > 90 then 'out of bounds'
              when not st_isvalid(%s) then st_isvalidreason(%s)::text
              when (%s) is null then 'empty'
         end
      """.stripMargin,
      paramPosition = Some(Seq(0, 1, 0, 0, 0, 0, 0, 0, 0))) _
  }

}
