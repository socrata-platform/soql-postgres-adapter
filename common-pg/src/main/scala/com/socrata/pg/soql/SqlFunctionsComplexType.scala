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
import com.socrata.soql.types.{SoQLPhone, SoQLValue, SoQLType}

trait SqlFunctionsComplexType {

  protected val funComplexTypeMap = Map[Function[SoQLType], FunCallToSql](
    TextToPhone -> textToPhone _,
    PhoneToPhoneNumber -> phoneSubColumn(0),
    PhoneToPhoneType -> phoneSubColumn(1),
    Phone -> formatCall("%s" + SqlFragments.Separator + "%s")
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

  private def phoneSubColumn(subColumnIndex: Int)
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

  private def camelCase(s: String): String = s.toLowerCase.capitalize
}
