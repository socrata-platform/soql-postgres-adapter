package com.socrata.pg.soql

import java.sql.PreparedStatement

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.pg.soql.SqlFunctions._
import com.socrata.pg.soql.Sqlizer._
import com.socrata.soql.functions.Function
import com.socrata.soql.functions.SoQLFunctions._
import com.socrata.soql.typed.StringLiteral
import com.socrata.soql.types.{SoQLPhone, SoQLType, SoQLUrl, SoQLValue}

/**
 * Complex types support does not have the same expressiveness of simple types.
 * It is more limited.
 *
 * Supported:
 *
 * select complex_type
 * function(complex_type_back_by_column.property)
 * complex_type_back_by_column binOp complex_type_ctor(literal, literal...)
 *
 * Unsupported:
 *
 * function(complex_type)
 * function(complex_type_back_by_literal.property)
 * complex_type_ctor(literal, literal, ...) binOp complex_type_ctor(literal, literal, ...)
 *
 * Making complex types more expressive requires a compound sql structure like json, heterogeneous array
 * during expression evaluation.
 * The way literals are handled also requires change.
 */
trait SqlFunctionsComplexType {

  protected val funComplexTypeMap = Map[Function[SoQLType], FunCallToSql](
    TextToPhone -> textToPhone _,
    PhoneToPhoneNumber -> subColumn(0),
    PhoneToPhoneType -> subColumn(1),
    Phone -> formatCall("%s" + SqlFragments.Separator + "%s"),

    TextToUrl -> textToUrl _,
    UrlToUrl -> subColumn(0),
    UrlToDescription -> subColumn(1),
    Url -> formatCall("%s" + SqlFragments.Separator + "%s")
  )

  private def textToPhone(fn: FunCall,
                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(strLit@StringLiteral(value: String, _)) =>
        val phone = value match {
          case SoQLPhone.phoneRx(phoneType, sep, phoneNumber) =>
            SoQLPhone(Option(phoneNumber).filter(_.nonEmpty),
                      Option(phoneType).filter(_.nonEmpty))
          case _ =>
            JsonUtil.parseJson[SoQLPhone](value).right.get
        }
        val subCols = Seq(phone.phoneNumber, phone.phoneType.map(camelCase))
        val sqls = subCols.map { subCol: Option[String] =>
          if (subCol.isDefined) { SqlParamPlaceHolder } else { SqlNull }
        }
        val setParam: Seq[SetParam] =
          subCols.filter(_.isDefined)
                 .map(_.get)
                 .map { subCol =>
                   (stmt: Option[PreparedStatement], pos: Int) => {
                     stmt.foreach(_.setString(pos, subCol))
                     Some(subCol)
                   }
                 }
        ParametricSql(sqls, setParams ++ setParam)
      case _ => throw new Exception("should never get anything but string literal")
    }
  }

  private def subColumn(subColumnIndex: Int)
                            (fn: FunCall,
                             rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                             typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                             setParams: Seq[SetParam],
                             ctx: Sqlizer.Context,
                             escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(col) =>
        val ParametricSql(sqls, params) = Sqlizer.sql(col)(rep, typeRep, setParams, ctx, escape)
        ParametricSql(Seq(sqls(subColumnIndex)), params) // Drop geom and keep only address
      case _ => throw new Exception("should never get here")
    }
  }

  private def textToUrl(fn: FunCall,
                        rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                        typeRep: Map[SoQLType, SqlColumnRep[SoQLType, SoQLValue]],
                        setParams: Seq[SetParam],
                        ctx: Sqlizer.Context,
                        escape: Escape): ParametricSql = {
    fn.parameters match {
      case Seq(strLit@StringLiteral(value: String, _)) =>
        val url = SoQLUrl.parseUrl(value).getOrElse(throw new Exception("cannot convert text to url"))
        val subCols = Seq(url.url, url.description)
        val sqls = subCols.map { subCol: Option[String] =>
          if (subCol.isDefined) { SqlParamPlaceHolder } else { SqlNull }
        }
        val setParam: Seq[SetParam] =
          subCols.filter(_.isDefined)
            .map(_.get)
            .map { subCol =>
              (stmt: Option[PreparedStatement], pos: Int) => {
                stmt.foreach(_.setString(pos, subCol))
                Some(subCol)
              }
            }
        ParametricSql(sqls, setParams ++ setParam)
      case _ => throw new Exception("should never get anything but string literal")
    }
  }

  private def camelCase(s: String): String = s.toLowerCase.capitalize
}
