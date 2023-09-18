package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.pg.analyzer2.metatypes.{InputMetaTypes, DatabaseNamesMetaTypes, CopyCache}

// Fake location helper: turn a map of what real columns make up
// fake-location columns in the InputMetaTypes world into the same map
// in the DatabaseNamesMetaTypes world.
object RewriteSubcolumns {
  def apply[MT <: MetaTypes with ({ type DatabaseColumnNameImpl = UserColumnId })](
    colMap: Map[
      types.DatabaseTableName[MT],
      Map[
        types.DatabaseColumnName[MT],
        Seq[Option[types.DatabaseColumnName[MT]]]
      ]
    ],
    copyCache: CopyCache[MT]
  ): Map[
    types.DatabaseTableName[DatabaseNamesMetaTypes],
    Map[
      types.DatabaseColumnName[DatabaseNamesMetaTypes],
      Seq[Option[types.DatabaseColumnName[DatabaseNamesMetaTypes]]]
    ]
  ] = {
    colMap.map { case (dtn, cols) =>
      val (copyInfo, newColMap) = copyCache(dtn).get // TODO proper error
      DatabaseNamesMetaTypes.rewriteDTN(DatabaseTableName(copyInfo)) -> cols.map { case (DatabaseColumnName(mainCol), auxCols) =>
        val rewrittenMainCol = DatabaseColumnName(newColMap.get(mainCol).get.physicalColumnBase) // TODO proper error
        val rewrittenAuxCols = auxCols.map {
          case Some(DatabaseColumnName(auxCol)) =>
            Some(DatabaseColumnName(newColMap.get(auxCol).get.physicalColumnBase)) // TODO proper error
          case None =>
            None
        }
        rewrittenMainCol -> rewrittenAuxCols
      }
    }
  }
}
