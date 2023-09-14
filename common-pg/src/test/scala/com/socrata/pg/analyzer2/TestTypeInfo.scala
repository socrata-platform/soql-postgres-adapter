package com.socrata.pg.analyzer2

import scala.util.parsing.input.Position

import com.socrata.soql.ast
import com.socrata.soql.collection.OrderedSet
import com.socrata.soql.environment.{TypeName, Provenance}
import com.socrata.soql.typechecker.{TypeInfo2, TypeInfoMetaProjection}
import com.socrata.soql.types.SoQLID
import com.socrata.soql.analyzer2._

object TestTypeInfo extends TypeInfo2[TestType, TestValue] {
  def metaProject[MT <: MetaTypes](
    implicit typeEv: TestType =:= MT#ColumnType,
    typeEvRev: MT#ColumnType =:= TestType,
    valueEv: TestValue =:= MT#ColumnValue,
    valueEvRev: MT#ColumnValue =:= TestValue
  ): TypeInfoMetaProjection[MT] =
    new TypeInfoMetaProjection[MT] {
      val unproject = TestTypeInfo.asInstanceOf[TypeInfo2[CT, CV]]

      implicit object hasType extends HasType[CV, CT] {
        def typeOf(v: CV) = v.typ
      }

      def boolType = TestBoolean

      def literalBoolean(b: Boolean, position: Position): Expr[MT] =
        LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(position))

      def potentialExprs(l: ast.Literal, currentPrimaryTable: Option[Provenance]): Seq[Expr[MT]] =
        l match {
          case ast.StringLiteral(s) =>
            val asInt: Option[CV] =
              try {
                Some(TestNumber(s.toInt))
              } catch {
                case _ : NumberFormatException => None
              }
            val asCompound = tryAsCompound(s)
            val asID = tryAsID(s, currentPrimaryTable)
            Seq[Option[CV]](Some(TestText(s)), asInt, asCompound, asID).flatten.map(LiteralValue[MT](_)(new AtomicPositionInfo(l.position)))
          case ast.NumberLiteral(n) => Seq(LiteralValue[MT](TestNumber(n.toInt))(new AtomicPositionInfo(l.position)))
          case ast.BooleanLiteral(b) => Seq(LiteralValue[MT](TestBoolean(b))(new AtomicPositionInfo(l.position)))
          case ast.NullLiteral() => typeParameterUniverse.iterator.map(NullLiteral[MT](_)(new AtomicPositionInfo(l.position))).toVector
        }

      private def tryAsID(s: String, currentPrimaryTable: Option[Provenance]): Option[CV] = {
        SoQLID.FormattedButUnobfuscatedStringRep.unapply(s).map { case SoQLID(n) =>
          val result = TestID(n)
          result.provenance = currentPrimaryTable
          result
        }
      }

      private def tryAsCompound(s: String) = {
        val result: Option[CV] = s.lastIndexOf("/") match {
          case -1 => None
          case slashPos =>
            val first = Some(s.substring(0, slashPos)).filter(_.nonEmpty)
            val second = s.substring(slashPos + 1)
            if(second.isEmpty) {
              Some(TestCompound(first, None))
            } else {
              try {
                Some(TestCompound(first, Some(second.toDouble)))
              } catch {
                case _: NumberFormatException =>
                  None
              }
            }
        }
        result match {
          case Some(TestCompound(None, None)) => None
          case other => other
        }
      }

      def updateProvenance(v: CV)(f: Provenance => Provenance): CV = {
        valueEvRev(v) match {
          case id: TestID =>
            val newId = id.copy()
            newId.provenance = id.provenance.map(f)
            newId
          case _ =>
            v
        }
      }
    }


  // Members declared in com.socrata.soql.typechecker.TypeInfoCommon

  def typeParameterUniverse: OrderedSet[TestType] = OrderedSet(
    TestText,
    TestNumber,
    TestBoolean,
    TestCompound,
    TestID
  )
  def isBoolean(typ: TestType): Boolean = typ == TestBoolean
  def isGroupable(typ: TestType): Boolean = true
  def isOrdered(typ: TestType): Boolean = typ.isOrdered
  def typeFor(name: TypeName): Option[TestType] = TestType.typesByName.get(name)
  def typeNameFor(typ: TestType): TypeName = typ.name
  def typeOf(value: TestValue): TestType = value.typ

  implicit object hasType extends HasType[TestValue, TestType] {
    def typeOf(v: TestValue) = v.typ
  }
}
