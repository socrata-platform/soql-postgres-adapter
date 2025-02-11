package com.socrata.pg.analyzer2

import java.math.{BigDecimal => JBigDecimal}

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer._
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLText, SoQLFloatingTimestamp, SoQLDate, SoQLNumber}
import com.socrata.soql.functions.{SoQLFunctions, SoQLTypeInfo}

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

class SoQLExprSqlizer[MT <: MetaTypes with metatypes.SoQLMetaTypesExt with ({ type ColumnType = SoQLType; type ColumnValue = SoQLValue; type ExtraContext = SoQLExtraContext })] extends ExprSqlizer[MT](
  new SoQLFunctionSqlizer[MT],
  new SoQLExprSqlFactory[MT]
) {
  import SoQLTypeInfo.hasType

  private object MonomorphicFunctions {
    val GetUtcDate = SoQLFunctions.GetUtcDate.monomorphic.get
    val ToFloatingTimestamp = SoQLFunctions.ToFloatingTimestamp.monomorphic.get

    val FloatingTimestampTruncYmd = SoQLFunctions.FloatingTimeStampTruncYmd.monomorphic.get
    val FloatingTimestampTruncYm = SoQLFunctions.FloatingTimeStampTruncYm.monomorphic.get
    val FloatingTimestampTruncY = SoQLFunctions.FloatingTimeStampTruncY.monomorphic.get

    val FloatingTimestampExtractDoW = SoQLFunctions.FloatingTimeStampExtractDow.monomorphic.get
    val FloatingTimestampExtractWoY = SoQLFunctions.FloatingTimeStampExtractWoy.monomorphic.get
    val FloatingTimestampExtractIsoY = SoQLFunctions.FloatingTimestampExtractIsoY.monomorphic.get

    val FixedTimestampZTruncYmd = SoQLFunctions.FixedTimeStampZTruncYmd.monomorphic.get
    val FixedTimestampZTruncYm = SoQLFunctions.FixedTimeStampZTruncYm.monomorphic.get
    val FixedTimestampZTruncY = SoQLFunctions.FixedTimeStampZTruncY.monomorphic.get

    val FixedTimestampTruncYmdAtTimeZone = SoQLFunctions.FixedTimeStampTruncYmdAtTimeZone.monomorphic.get
    val FixedTimestampTruncYmAtTimeZone = SoQLFunctions.FixedTimeStampTruncYmAtTimeZone.monomorphic.get
    val FixedTimestampTruncYAtTimeZone = SoQLFunctions.FixedTimeStampTruncYAtTimeZone.monomorphic.get

    val FloatingTimestampYearField = SoQLFunctions.FloatingTimestampYearField.monomorphic.get
    val FloatingTimestampMonthField = SoQLFunctions.FloatingTimestampMonthField.monomorphic.get
    val FloatingTimestampDayField = SoQLFunctions.FloatingTimestampDayField.monomorphic.get

    val FloatingTimestampDateField = SoQLFunctions.FloatingTimestampDateField.monomorphic.get
    val FloatingTimestampDayOfWeekField = SoQLFunctions.FloatingTimestampDayOfWeekField.monomorphic.get
    val FloatingTimestampWeekOfYearField = SoQLFunctions.FloatingTimestampWeekOfYearField.monomorphic.get
    val FloatingTimestampIsoYearField = SoQLFunctions.FloatingTimestampIsoYearField.monomorphic.get
  }

  private sealed abstract class NowInZone {
    val zoneName: String

    def asExpr(tsp: TimestampProvider, pos: PositionInfo): Option[LiteralValue]
  }
  private object NowInZone {
    case class FullPrecision(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowInZone(zoneName).map { v => LiteralValue[MT](SoQLFloatingTimestamp(v))(pos.asAtomic) }
    }
    case class DayPrecision(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowTruncDay(zoneName).map { v => LiteralValue[MT](SoQLFloatingTimestamp(v))(pos.asAtomic) }
    }
    case class MonthPrecision(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowTruncMonth(zoneName).map { v => LiteralValue[MT](SoQLFloatingTimestamp(v))(pos.asAtomic) }
    }
    case class YearPrecision(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowTruncYear(zoneName).map { v => LiteralValue[MT](SoQLFloatingTimestamp(v))(pos.asAtomic) }
    }
    case class Date(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowDate(zoneName).map { v => LiteralValue[MT](SoQLDate(v))(pos.asAtomic) }
    }
    case class OnlyDay(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractDay(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }
    case class OnlyMonth(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractMonth(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }
    case class OnlyYear(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractYear(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }
    case class OnlyDayOfWeek(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractDayOfWeek(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }
    case class OnlyWeekOfYear(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractWeekOfYear(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }
    case class OnlyIsoYear(zoneName: String) extends NowInZone {
      override def asExpr(tsp: TimestampProvider, pos: PositionInfo) =
        tsp.nowExtractIsoYear(zoneName).map { v => LiteralValue[MT](SoQLNumber(new JBigDecimal(v)))(pos.asAtomic) }
    }

    private object FixedNow {
      def unapply(e: Expr): Boolean = {
        e match {
          case FunctionCall(MonomorphicFunctions.GetUtcDate, Seq()) =>
            true
          case _ =>
            false
        }
      }
    }

    private object LiteralZone {
      def unapply(e: Expr): Option[String] = {
        e match {
          case LiteralValue(SoQLText(s)) =>
            Some(s)
          case _ =>
            None
        }
      }
    }

    private object FloatNow {
      def unapply(e: Expr): Option[String] = {
        e match {
          case FunctionCall(MonomorphicFunctions.ToFloatingTimestamp, Seq(FixedNow(), LiteralZone(zoneName))) =>
            Some(zoneName)
          case _ =>
            None
        }
      }
    }

    def unapply(e: Expr): Option[NowInZone] = {
      e match {
        case FloatNow(zoneName) =>
          Some(FullPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampTruncYmd, Seq(FloatNow(zoneName))) =>
          Some(DayPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampTruncYm, Seq(FloatNow(zoneName))) =>
          Some(MonthPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampTruncY, Seq(FloatNow(zoneName))) =>
          Some(YearPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FixedTimestampZTruncYmd, Seq(FixedNow())) =>
          Some(DayPrecision("UTC"))
        case FunctionCall(MonomorphicFunctions.FixedTimestampZTruncYm, Seq(FixedNow())) =>
          Some(MonthPrecision("UTC"))
        case FunctionCall(MonomorphicFunctions.FixedTimestampZTruncY, Seq(FixedNow())) =>
          Some(YearPrecision("UTC"))
        case FunctionCall(MonomorphicFunctions.FixedTimestampTruncYmdAtTimeZone, Seq(FixedNow(), LiteralZone(zoneName))) =>
          Some(DayPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FixedTimestampTruncYmAtTimeZone, Seq(FixedNow(), LiteralZone(zoneName))) =>
          Some(MonthPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FixedTimestampTruncYAtTimeZone, Seq(FixedNow(), LiteralZone(zoneName))) =>
          Some(YearPrecision(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampDateField, Seq(FloatNow(zoneName))) =>
          Some(Date(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampDateField, Seq(FloatNow(zoneName))) =>
          Some(Date(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampYearField, Seq(FloatNow(zoneName))) =>
          Some(OnlyYear(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampMonthField, Seq(FloatNow(zoneName))) =>
          Some(OnlyMonth(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampDayField, Seq(FloatNow(zoneName))) =>
          Some(OnlyDay(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampDayOfWeekField, Seq(FloatNow(zoneName))) =>
          Some(OnlyDayOfWeek(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampExtractDoW, Seq(FloatNow(zoneName))) =>
          Some(OnlyDayOfWeek(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampWeekOfYearField, Seq(FloatNow(zoneName))) =>
          Some(OnlyDayOfWeek(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampExtractWoY, Seq(FloatNow(zoneName))) =>
          Some(OnlyDayOfWeek(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampIsoYearField, Seq(FloatNow(zoneName))) =>
          Some(OnlyIsoYear(zoneName))
        case FunctionCall(MonomorphicFunctions.FloatingTimestampExtractIsoY, Seq(FloatNow(zoneName))) =>
          Some(OnlyIsoYear(zoneName))
        case _ =>
          None
      }
    }
  }

  private class SoQLContextedExprSqlizer(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ) extends DefaultContextedExprSqlizer(availableSchemas, selectListIndices, sqlizerCtx) {
    override def sqlize(e: Expr): ExprSql = {
      // The FuncallSqlizer operates bottom-up, so if we want to do
      // some kinds of optimization - in particular, if we want to
      // notice that only a coarse part of a call to `get_utc_date()`
      // is used - we need to intercept it on the way down, which
      // happens here.
      e match {
        case expr@NowInZone(thing) =>
          thing.asExpr(sqlizerCtx.extraContext.timestampProvider, expr.position) match {
            case Some(replacement) => super.sqlize(replacement)
            case None => super.sqlize(expr)
          }
        case other => super.sqlize(other)
      }
    }
  }

  override def withContext(
    availableSchemas: AvailableSchemas,
    selectListIndices: IndexedSeq[SelectListIndex],
    sqlizerCtx: Sqlizer.DynamicContext[MT]
  ): ExprSqlizer.Contexted[MT] =
    new SoQLContextedExprSqlizer(availableSchemas, selectListIndices, sqlizerCtx)

}
