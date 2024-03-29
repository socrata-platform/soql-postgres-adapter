package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.{SoQLValue, SoQLID, SoQLVersion}
import com.socrata.soql.types.obfuscation.CryptProvider

trait SoQLValueDebugHelper {
  implicit def hasDocCV(implicit cryptProviderProvider: CryptProviderProvider) = new HasDoc[SoQLValue] {
    def docOf(cv: SoQLValue) =
      cv match {
        case id@SoQLID(_) =>
          id.doc(cryptProviderFor(id.provenance))
        case ver@SoQLVersion(_) =>
          ver.doc(cryptProviderFor(ver.provenance))
        case other =>
          other.doc(CryptProvider.zeros)
      }

    private def cryptProviderFor(provenance: Option[Provenance]): CryptProvider =
      provenance.flatMap(cryptProviderProvider.forProvenance).getOrElse(CryptProvider.zeros)
  }
}
