package com.socrata.pg.analyzer2

import com.socrata.soql.analyzer2.CanonicalName
import com.socrata.soql.types.obfuscation.CryptProvider

trait CryptProviderProvider extends (CanonicalName => Option[CryptProvider]) {
  def apply(cn: CanonicalName) = {
    val CanonicalName(underlying) = cn
    forProvenance(underlying)
  }

  def forProvenance(provenenace: String): Option[CryptProvider]
}

object CryptProviderProvider {
  val empty = new CryptProviderProvider {
    def forProvenance(provenance: String) = None
  }
}
