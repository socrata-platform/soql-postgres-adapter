package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.obfuscation.CryptProvider

import com.socrata.datacoordinator.truth.metadata.DatasetInfo

import com.socrata.pg.analyzer2.metatypes.{DatabaseMetaTypes, DatabaseNamesMetaTypes}

object CryptProvidersByDatabaseNamesProvenance extends StatementUniverse[DatabaseMetaTypes] {
  def apply(stmt: Statement): CryptProviderProvider = {
    new CryptProviderProvider {
      val map = findCryptProvidersS(stmt, Map.empty).iterator.map { case (provenance, dsInfo) =>
        provenance -> dsInfo.cryptProvider
      }.toMap

      def forProvenance(prov: Provenance) =
        map.get(prov)
    }
  }

  private type Acc = Map[Provenance, DatasetInfo]

  private def findCryptProvidersAF(from: AtomicFrom, acc: Acc): Acc =
    from match {
      case FromStatement(stmt, _, _, _) => findCryptProvidersS(stmt, acc)
      case _: FromSingleRow => acc
      case FromTable(dtn@DatabaseTableName(copyInfo), _, _, _, _, _) =>
        val prov = DatabaseNamesMetaTypes.provenanceMapper.toProvenance(DatabaseNamesMetaTypes.rewriteDTN(dtn))
        acc.get(prov) match {
          case Some(dsInfo) =>
            assert(copyInfo.datasetInfo.systemId == dsInfo.systemId)
            acc
          case None =>
            acc + (prov -> copyInfo.datasetInfo)
        }
    }

  private def findCryptProvidersF(from: From, acc: Acc): Acc =
    from.reduce[Acc](
      findCryptProvidersAF(_, acc),
      { (acc, j) => findCryptProvidersAF(j.right, acc) }
    )

  private def findCryptProvidersS(stmt: Statement, acc: Acc): Acc =
    stmt match {
      case CombinedTables(_op, left, right) =>
        findCryptProvidersS(left, findCryptProvidersS(right, acc))
      case CTE(_, _, q1, _, q2) =>
        findCryptProvidersS(q1, findCryptProvidersS(q2, acc))
      case s: Select =>
        findCryptProvidersF(s.from, acc)
      case v: Values =>
        acc
    }
}
