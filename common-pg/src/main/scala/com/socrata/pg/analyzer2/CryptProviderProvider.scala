package com.socrata.pg.analyzer2

import com.socrata.soql.environment.Provenance
import com.socrata.soql.types.obfuscation.CryptProvider

trait CryptProviderProvider {
  def forProvenance(provenenace: Provenance): Option[CryptProvider]
}

object CryptProviderProvider {
  val empty = new CryptProviderProvider {
    def forProvenance(provenance: Provenance) = None
  }
}
