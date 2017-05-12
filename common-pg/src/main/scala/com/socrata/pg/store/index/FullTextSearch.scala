package com.socrata.pg.store.index

import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.typed.CoreExpr
import com.socrata.soql.types.{SoQLArray, SoQLObject, SoQLText, SoQLType}

trait FullTextSearch[CT] {

  protected val SearchableTypes: Set[CT]

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
    val cols = selection.view.filter(x => SearchableTypes.contains(x._2.typ)).map { case (columnName, expr) =>
      val name = ctx.map(c => qualify(columnName.name, c)).getOrElse(columnName.name)
      coalesce(name)
    }
    toTsVector(cols.toSeq)
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
}
