package com.socrata.pg.store.index

import com.socrata.datacoordinator.truth.metadata.ColumnInfo
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

trait IndexSupport[CT, CV] {

  def repForIndex: ColumnInfo[CT] => SqlColumnRep[CT, CV] with Indexable[CT]

}
