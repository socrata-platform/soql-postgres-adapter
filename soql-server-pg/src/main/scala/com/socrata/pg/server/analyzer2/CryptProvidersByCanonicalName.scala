package com.socrata.pg.server.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.types.obfuscation.CryptProvider

import com.socrata.datacoordinator.truth.metadata.DatasetInfo

import com.socrata.pg.analyzer2.CryptProviderProvider

object CryptProvidersByCanonicalName extends StatementUniverse[DatabaseMetaTypes] {
  def apply(stmt: Statement): CryptProviderProvider = {
    new CryptProviderProvider {
      val map = findCryptProvidersS(stmt, Map.empty).iterator.map { case (CanonicalName(canonName), dsInfo) =>
        canonName -> new CryptProvider(dsInfo.obfuscationKey)
      }.toMap

      def forProvenance(s: String) =
        map.get(s)
    }
  }

  private type Acc = Map[CanonicalName, DatasetInfo]

  private def findCryptProvidersAF(from: AtomicFrom, acc: Acc): Acc =
    from match {
      case FromStatement(stmt, _, _, _) => findCryptProvidersS(stmt, acc)
      case _: FromSingleRow => acc
      case FromTable(DatabaseTableName(copyInfo), canonicalName, _, _, _, _, _) =>
        acc.get(canonicalName) match {
          case Some(dsInfo) =>
            assert(copyInfo.datasetInfo.systemId == dsInfo.systemId, s"Canonical name $canonicalName referred to two different datasets ${dsInfo.systemId} and ${copyInfo.datasetInfo.systemId}")
            acc
          case None =>
            acc + (canonicalName -> copyInfo.datasetInfo)
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
