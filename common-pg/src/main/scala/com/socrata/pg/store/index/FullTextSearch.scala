package com.socrata.pg.store.index

import com.socrata.datacoordinator.truth.sql.SqlColumnCommonRep
import com.socrata.soql.types.{SoQLArray, SoQLObject, SoQLText, SoQLType}

trait FullTextSearch {

  private val SearchableTypes: Set[SoQLType] = Set(SoQLText, SoQLObject, SoQLArray)

  /**
   * Search vector can be None when there are no columns that support full text search.
   * @param reps
   * @return
   */
  def searchVector(reps: Seq[SqlColumnCommonRep[SoQLType]]): Option[String] = {
    val repsSearchableTypes = reps.filter(r => SearchableTypes.contains(r.representedType))
    val phyCols = repsSearchableTypes.flatMap(rep => rep.physColumns.map(phyCol => s"coalesce($phyCol,'')"))
    if (phyCols.isEmpty) None
    else Some(phyCols.sorted.mkString("to_tsvector('english', ", " || ' ' || ", ")"))
  }
}
