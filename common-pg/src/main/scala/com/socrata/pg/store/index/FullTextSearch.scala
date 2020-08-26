package com.socrata.pg.store.index

import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.pg.soql.Sqlizer._
import com.socrata.pg.soql.SqlizerContext._
import com.socrata.soql.environment.TableName


trait FullTextSearch[CT] {

  val SearchableTypes: Set[CT]
  val SearchableNumericTypes: Set[CT]

  /**
   * Search vector can be None when there are no columns that support full text search.
   * @param reps
   * @return
   */
  def searchVector(reps: Seq[SqlColumnCommonRep[CT]], ctx: Option[Context]): Option[String] = {
    val repsSearchableTypes = reps.filter(r => SearchableTypes.contains(r.representedType))
    val phyCols = repsSearchableTypes.flatMap(rep => rep.physColumns.map(phyCol =>
      ctx.map(qualify(phyCol, _)).getOrElse(phyCol)
    ))
    toTsVector(phyCols.zip(phyCols))
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
  def toTsVector(colSortAndCols: Seq[(String, String)]): Option[String] = {
    if (colSortAndCols.isEmpty) None
    else Some(colSortAndCols.sortBy(_._1).map(x => coalesce(x._2)).mkString("to_tsvector('english', ", " || ' ' || ", ")"))
  }
}
