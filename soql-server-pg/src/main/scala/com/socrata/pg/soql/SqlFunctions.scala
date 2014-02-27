package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.functions.Function
import com.socrata.soql.types._
import com.socrata.soql.typed.{NumberLiteral, StringLiteral, FunctionCall}
import com.socrata.soql.types.SoQLID.{StringRep => SoQLIDRep}
import com.socrata.soql.types.SoQLVersion.{StringRep => SoQLVersionRep}


object SqlFunctions {

  import SoQLFunctions._

  import Sqlizer._

  type FunCall = FunctionCall[UserColumnId, SoQLType]

  type FunCallToSql = (FunCall, Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], Seq[SetParam], SoQLIDRep, SoQLVersionRep) => ParametricSql

  def apply(function: Function[SoQLType]) = funMap(function)

  private val funMap = Map[Function[SoQLType], FunCallToSql](
    IsNull -> formatCall("%s is null") _,
    IsNotNull -> formatCall("%s is not null") _,
    Not -> formatCall("not %s") _,
    In -> naryish("in") _, // TODO - detect and handle row id decryption
    NotIn -> naryish("not in") _, // TODO - detect and handle row id decryption
    Eq -> infix("=") _, // TODO - detect and handle row id decryption
    EqEq -> infix("=") _, // TODO - detect and handle row id decryption
    TextToFloatingTimestamp -> todo _,
    TextToFixedTimestamp -> todo _,
    Neq -> infix("!=") _,
    BangEq -> infix("!=") _,
    And -> infix("and") _,
    Or -> infix("or") _,
    UnaryMinus -> formatCall("-%s") _,
    NotBetween -> formatCall("not %s between %s and %s") _,
    WithinCircle -> todo _,
    WithinBox -> todo _,
    Between -> formatCall("%s between %s and %s") _,
    Lt -> infix("<") _,
    Lte -> infix("<=") _,
    Gt -> infix(">")_,
    Gte -> infix(">=") _,
    TextToRowIdentifier -> decryptString(SoQLID) _,
    TextToRowVersion -> decryptString(SoQLVersion) _,
    Like -> infix("like") _,
    NotLike -> infix("not like") _,
    StartsWith -> infix("like") _, // TODO - Need to add suffix % to the 2nd operand.
    Contains -> infix("like") _,  // TODO - Need to add prefix % and suffix % to the 2nd operand.
    Concat -> infix("||") _
    // TODO: Complete the function list.
  )

  private def infix(fnName: String)
                   (fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = {
    val ParametricSql(l, setParamsL) = fn.parameters(0).sql(rep, setParams, idRep, verRep)
    val ParametricSql(r, setParamsLR) = fn.parameters(1).sql(rep, setParamsL, idRep, verRep)
    val s = s"$l $fnName $r"
    ParametricSql(s, setParamsLR)
  }

  private def nary(fnName: String)
                  (fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, idRep, verRep)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def naryish(fnName: String)
                     (fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = {

    val ParametricSql(head, setParamsHead) = fn.parameters.head.sql(rep, setParams, idRep, verRep)

    val sqlFragsAndParams = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, idRep, verRep)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(head + " " + fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def formatCall(template: String)
                        (fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2, idRep, verRep)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(template.format(sqlFragsAndParams._1), sqlFragsAndParams._2)
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

  private def decryptString(typ: SoQLType)(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = {
    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      param match {
        case strLit@StringLiteral(value: String, _) =>
          val numLit = decryptToNumLit(typ)(idRep, verRep, strLit)
          val ParametricSql(sql, newSetParams) = numLit.sql(rep, acc._2, idRep, verRep)
          (acc._1 :+ sql, newSetParams)
        case unexpected =>
          throw new Exception("Row id is not string literal")
      }
    }
    ParametricSql(sqlFragsAndParams._1.mkString(","), sqlFragsAndParams._2)
  }

  private def todo(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam], idRep: SoQLIDRep, verRep: SoQLVersionRep): ParametricSql = ???
}
