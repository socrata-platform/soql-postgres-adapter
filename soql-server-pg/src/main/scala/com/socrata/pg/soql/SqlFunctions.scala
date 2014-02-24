package com.socrata.pg.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.functions.SoQLFunctions
import com.socrata.soql.environment.FunctionName
import com.socrata.soql.types._
import com.socrata.soql.typed.FunctionCall
import com.socrata.datacoordinator.truth.sql.SqlColumnRep


object SqlFunctions {

  import SoQLFunctions._

  import Sqlizer._

  type FunCall = FunctionCall[UserColumnId, SoQLType]

  def apply(functionName: FunctionName) = funMap.get(functionName)

  private val funMap = Map(
    IsNull.name -> formatCall("%s is null") _,
    IsNotNull.name -> formatCall("%s is not null") _,
    Not.name -> formatCall("not %s") _,
    In.name -> naryish("in") _, // TODO - detect and handle row id decryption
    NotIn.name -> naryish("not in") _, // TODO - detect and handle row id decryption
    Eq.name -> infix("=") _, // TODO - detect and handle row id decryption
    EqEq.name -> infix("=") _, // TODO - detect and handle row id decryption
    TextToFloatingTimestamp.name -> todo _,
    TextToFixedTimestamp.name -> todo _,
    Neq.name -> infix("!=") _,
    BangEq.name -> infix("!=") _,
    And.name -> infix("and") _,
    Or.name -> infix("or") _,
    UnaryMinus.name -> formatCall("-%s") _,
    NotBetween.name -> formatCall("not %s between %s and %s") _,
    WithinCircle.name -> todo _,
    WithinBox.name -> todo _,
    Between.name -> formatCall("%s between %s and %s") _,
    Lt.name -> infix("<") _,
    Lte.name -> infix("<=") _,
    Gt.name -> infix(">")_,
    Gte.name -> infix(">=") _,
    Like.name -> infix("like") _,
    NotLike.name -> infix("not like") _,
    StartsWith.name -> infix("like") _, // TODO - Need to add suffix % to the 2nd operand.
    Contains.name -> infix("like") _,  // TODO - Need to add prefix % and suffix % to the 2nd operand.
    Concat.name -> infix("||") _
    // TODO: Complete the function list.
  )

  private def infix(fnName: String)(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]): ParametricSql = {
    val ParametricSql(l, setParamsL) = fn.parameters(0).sql(rep, setParams)
    val ParametricSql(r, setParamsLR) = fn.parameters(1).sql(rep, setParamsL)
    val s = s"$l $fnName $r"
    ParametricSql(s, setParamsLR)
  }

  private def nary(fnName: String)(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def naryish(fnName: String)(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]): ParametricSql = {
    val ParametricSql(head, setParamsHead) = fn.parameters.head.sql(rep, setParams)

    val sqlFragsAndParams = fn.parameters.tail.foldLeft(Tuple2(Seq.empty[String], setParamsHead)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(sqlFragsAndParams._1.mkString(head + " " + fnName + "(", ",", ")"), sqlFragsAndParams._2)
  }

  private def formatCall(template: String)(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]): ParametricSql = {

    val sqlFragsAndParams = fn.parameters.foldLeft(Tuple2(Seq.empty[String], setParams)) { (acc, param) =>
      val ParametricSql(sql, newSetParams) = param.sql(rep, acc._2)
      (acc._1 :+ sql, newSetParams)
    }

    ParametricSql(template.format(sqlFragsAndParams._1), sqlFragsAndParams._2)
  }

  private def todo(fn: FunCall, rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[SetParam]): ParametricSql = ???
}
