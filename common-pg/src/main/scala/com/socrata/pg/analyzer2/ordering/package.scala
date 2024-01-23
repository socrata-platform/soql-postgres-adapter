package com.socrata.pg.analyzer2

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}

package object ordering {
  implicit object datasetInternalNameOrdering extends Ordering[DatasetInternalName] {
    def compare(a: DatasetInternalName, b: DatasetInternalName) =
      a.instance.compare(b.instance) match {
        case 0 => a.datasetId.underlying.compare(b.datasetId.underlying)
        case n => n
      }
  }

  implicit object userColumnIdOrdering extends Ordering[UserColumnId] {
    def compare(a: UserColumnId, b: UserColumnId) = {
      a.underlying.compare(b.underlying)
    }
  }
}
