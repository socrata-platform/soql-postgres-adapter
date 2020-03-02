package com.socrata.pg.soql

import scala.util.parsing.input.{NoPosition, Position}

import java.sql.PreparedStatement

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.io.CompactJsonWriter

import com.socrata.datacoordinator.truth.sql.SqlColumnReadRep
import com.socrata.datacoordinator.truth.sql.SqlColumnWriteRep
import com.socrata.datacoordinator.truth.json.JsonColumnWriteRep
import com.socrata.soql.types.{SoQLType, SoQLValue}

// A collection of columns which together make up "the state of a soql
// column during some intermediate compuation".  E.g., for the expression
//   someCompoundColA == someCompoundColB
// this will pass through one of these where the fragments are the pairwise
// comparison of the subcols
abstract class IntermediateSoQLColumn { self =>
  def sqlFragments: Seq[ParametricSqlBuilder] // one element per sub-column
  def mapFragments(f: ParametricSqlBuilder => ParametricSqlBuilder): IntermediateSoQLColumn =
    new IntermediateSoQLColumn {
      def sqlFragments = self.sqlFragments.map(f)
    }
}

// This represents a collection of columns which together make up a
// single SoQL-level column
abstract class SoQLColumn extends IntermediateSoQLColumn {
  def typ: SoQLType
}

object SoQLColumn {
  def columnRef(rep: SqlColumnReadRep[SoQLType, SoQLValue], pos: Position): SoQLColumn =
    new SoQLColumn {
      def typ = rep.representedType
      def sqlFragments = rep.physColumns.map { subcol =>
        new ParametricSqlBuilder().appendRawSql(subcol, pos)
      }
    }

  def literal(rep: SqlColumnWriteRep[SoQLType, SoQLValue], asJson: SoQLValue => JValue, value: SoQLValue, pos: Position): SoQLColumn =
    new SoQLColumn {
      def typ = rep.representedType
      val sqlFragments = (rep.insertPlaceholders, rep.prepareInserts).zipped.map { (placeholderStr, prep) =>
        new ParametricSqlBuilder().
          appendParameter(placeholderStr, prep, value, asJson, pos)
      }
    }
}

object ParametricSqlBuilder {
  private sealed class Fragment

  private sealed abstract class ConcreteFragment extends Fragment {
    def sql: String
    def position: Position
  }

  private abstract class Parameter extends ConcreteFragment {
    def show: JValue
  }

  private case class SoQLParameter(sql: String, prep: (PreparedStatement, SoQLValue, Int) => Unit, value: SoQLValue, asJValue: SoQLValue => JValue, pos: Position) extends Parameter {
    def show = asJValue(value)
  }

  private case class RawParameter(sql: String, prep: (PreparedStatement, Int) => Int, asJValue: JValue, pos: Position) extends Parameter {
    def show = asJValue
  }

  private case class Code(sql: String, position: Position) extends ConcreteFragment

  private case class MultiFragment(fragments: Vector[Fragment]) extends Fragment

  private def flatten(fragments: TraversableOnce[Fragment]): Vector[ConcreteFragment] = {
    val result = Vector.newBuilder[ConcreteFragment]
    def loop(fragments: TraversableOnce[Fragment]) {
      for(fragment <- fragments) {
        fragment match {
          case cf: ConcreteFragment => result += cf
          case MultiFragment(pieces) => loop(pieces)
        }
      }
    }
    loop(fragments)
    result.result()
  }

  private def finalSql(fragments: Iterable[ConcreteFragment]): String =
    fragments.iterator.map(_.sql).mkString(" ")

  private def paramsIterator(fragments: Iterable[ConcreteFragment]): Iterator[Parameter] =
    fragments.iterator.collect { case p: Parameter => p }

  private def finalSetter(fragments: TraversableOnce[ConcreteFragment]): PreparedStatement => Unit = { stmt =>
    fragments.foldLeft(1) { (col, fragment) =>
      fragment match {
        case param: SoQLParameter =>
          param.prep(stmt, param.value, col)
          col + 1
        case param: RawParameter =>
          param.prep(stmt, col)
        case _ : Code =>
          col
      }
    }
  }

  trait Append {
    def appendTo(builder: ParametricSqlBuilder)
  }

  object implicits {
    implicit class ParametricSqlSeq(xs: Seq[ParametricSqlBuilder]) {
      def mkSql(prefix: String, infix: String, suffix: String): ParametricSqlBuilder =
        new ParametricSqlBuilder().
          appendRawSql(prefix).
          interspersingRawSql(xs, infix).
          appendRawSql(suffix)

      def mkSql(infix: String): ParametricSqlBuilder =
        new ParametricSqlBuilder().interspersingRawSql(xs, infix)
    }
  }
}

class ParametricSqlBuilder extends ParametricSqlBuilder.Append {
  import ParametricSqlBuilder._

  private val builder = Vector.newBuilder[Fragment]

  def appendRawSql(s: String, pos: Position = NoPosition): this.type = {
    if(s.nonEmpty) builder += Code(s, pos)
    this
  }

  def appendColumn(col: SoQLColumn, separator: String): this.type =
    interspersingRawSql(col.sqlFragments, separator)

  def appendParameter(sql: String, prep: (PreparedStatement, SoQLValue, Int) => Unit, value: SoQLValue, asJValue: SoQLValue => JValue, pos: Position): this.type = {
    builder += SoQLParameter(sql, prep, value, asJValue, pos)
    this
  }

  def appendJsonbParameter(v: JValue, pos: Position = NoPosition): this.type = {
    builder += RawParameter("(? :: jsonb)", (ps, off) => { ps.setString(off, CompactJsonWriter.toString(v)); off + 1 }, v, pos)
    this
  }

  def appendParametricSql(that: Append): this.type = {
    that.appendTo(this)
    this
  }

  def appendTo(that: ParametricSqlBuilder) {
    val fragments = that.builder.result()
    if(fragments.nonEmpty) that.builder += MultiFragment(fragments)
  }

  def ++=(that: ParametricSqlBuilder): this.type = appendParametricSql(that)

  // annoyingly, this is the only thing that _doesn't_ modify this value
  def surrounded(prefix: String, suffix: String): ParametricSqlBuilder =
    new ParametricSqlBuilder().appendRawSql(prefix).appendParametricSql(this).appendRawSql(suffix)

  def interspersingRawSql(things: TraversableOnce[ParametricSqlBuilder], separator: String): this.type = {
    var didOne = false
    for(thing <- things) {
      if(didOne) appendRawSql(separator)
      else didOne = true
      appendParametricSql(thing)
    }
    this
  }

  override def toString(): String = {
    val fragments = flatten(builder.result())

    val sql = finalSql(fragments)
    val params = paramsIterator(fragments)

    s"$sql; params: ${params.map(_.show).mkString(", ")}"
  }

  def result(): (String, (PreparedStatement => Unit)) = {
    // TODO: annotate the resulting SQL with position information for error reporting
    val fragments = flatten(builder.result())

    (finalSql(fragments), finalSetter(fragments))
  }
}
