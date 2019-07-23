package com.socrata.pg.store.index

import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.SoQLUrl

trait FullTextSearch[CT] {

  protected val SearchableTypes: Set[CT]
  protected val SearchableNumericTypes: Set[CT]

  /**
   * Search vector can be None when there are no columns that support full text search.
   * @param reps
   * @return
   */
  def searchVector(reps: Seq[SqlColumnCommonRep[CT]], ctx: Option[Context]): Option[String] = {
    val repsSearchableTypes = reps.filter(r => SearchableTypes.contains(r.representedType))
    val phyCols = repsSearchableTypes.flatMap(rep => rep.physColumns.map(phyCol =>
      coalesce(ctx.map(qualify(phyCol, _)).getOrElse(phyCol))
    ))
    toTsVector(phyCols)
  }

  def searchVector(selection: OrderedMap[ColumnName, CoreExpr[_, CT]], ctx: Option[Context]): Option[String] = {
    val cols = selection.view.filter(x => SearchableTypes.contains(x._2.typ)).flatMap { case (columnName, expr) =>
      val subColumns = typeSubColumns(expr.typ)
      subColumns.map { sc =>
        val subColumnName = s"${columnName.name}${sc}"
        val qualifiedSubColumnName = ctx.map(c => qualify(subColumnName, c)).getOrElse(subColumnName)
        coalesce(qualifiedSubColumnName)
      }
    }
    toTsVector(cols.toSeq)
  }

  def searchNumericVector(selection: OrderedMap[ColumnName, CoreExpr[_, CT]], ctx: Option[Context]): Seq[String] = {
    val cols = selection.view.filter(x => SearchableNumericTypes.contains(x._2.typ)).map { case (columnName, expr) =>
      ctx.map(c => qualify(columnName.name, c)).getOrElse(columnName.name)
    }
    cols.toSeq
  }

  def searchNumericVector(reps: Seq[SqlColumnCommonRep[CT]], ctx: Option[Context]): Seq[String] = {
    val repsSearchableTypes = reps.filter(r => SearchableNumericTypes.contains(r.representedType))
    val phyCols = repsSearchableTypes.flatMap(rep => rep.physColumns.map(phyCol =>
      ctx.map(qualify(phyCol, _)).getOrElse(phyCol))
    )
    phyCols
  }

  private def qualify(column: String, ctx: Context): String = {
    if (ctx(InnermostSoql) == true) {
      val tableMap = ctx(TableAliasMap).asInstanceOf[Map[String, String]]
      s"""${tableMap(TableName.PrimaryTable.qualifier)}."${column}""""
    } else {
      s""""${column}""""
    }
  }

  private def coalesce(col: String): String = {
    s"coalesce($col,'')"
  }

  private def toTsVector(cols: Seq[String]): Option[String] = {
    if (cols.isEmpty) None
    else Some(cols.sorted.mkString("to_tsvector('english', ", " || ' ' || ", ")"))
  }

  private def typeSubColumns(t: CT): Seq[String] = {
    t match {
      case _: SoQLUrl.type => Seq("_url", "_description")
      case _ => Seq("")
    }
  }
}
