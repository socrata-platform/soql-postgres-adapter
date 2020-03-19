package com.socrata.pg.store.index

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed.{ColumnRef, CoreExpr}
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
    toTsVector(phyCols.zip(phyCols))
  }

  def searchVector(selection: OrderedMap[ColumnName, CoreExpr[UserColumnId, CT]], ctx: Option[Context]): Option[String] = {
    val colSortAndCols: Seq[(String, String)] = selection.view.filter(x => SearchableTypes.contains(x._2.typ)).flatMap { case (columnName, expr) =>
      val subColumns = typeSubColumns(expr.typ)

      // make sure the ts vector column ordering matches the index, if possible
      // index is created with the true column name (ex: u_6tcq_mbxm_28) alphabetically
      val columnNameForSort: String = expr match {
        case cr: ColumnRef[UserColumnId, CT] => cr.column.underlying // this is not strictly the true column name, but should be sufficient for ordering
        case _ => columnName.name // if these aren't all ColumnRefs, sorting isn't going to get us an index match anyway
      }

      subColumns.map { sc =>
        val subColumnName = s"${columnName.name}${sc}"
        val qualifiedSubColumnName = ctx.map(c => qualify(subColumnName, c)).getOrElse(subColumnName)

        (s"${columnNameForSort}${sc}", coalesce(qualifiedSubColumnName))
      }
    }.toSeq

    toTsVector(colSortAndCols)
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

  /**
    * First element of the tuple indicates sort order for the column
    */
  private def toTsVector(colSortAndCols: Seq[(String, String)]): Option[String] = {
    if (colSortAndCols.isEmpty) None
    else Some(colSortAndCols.sortBy(_._1).map(_._2).mkString("to_tsvector('english', ", " || ' ' || ", ")"))
  }

  private def typeSubColumns(t: CT): Seq[String] = {
    t match {
      case _: SoQLUrl.type => Seq("_url", "_description")
      case _ => Seq("")
    }
  }
}
