package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.sqlizer._
import com.socrata.datacoordinator.common

import com.socrata.pg.analyzer2.metatypes.{DatabaseNamesMetaTypes, AugmentedTableName}

object SoQLSqlizer extends Sqlizer[DatabaseNamesMetaTypes](
  new SoQLExprSqlizer,
  Namespaces,
  new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true, SoQLRewriteSearch.simpleDcnComparator),
  DatabaseNamesMetaTypes.provenanceMapper,
  {
    case DatabaseTableName(AugmentedTableName.RollupTable(_)) => true
    case DatabaseTableName(AugmentedTableName.TTable(_, _)) => false
  },
  (sqlizer, physicalTableFor, extraContext) => new SoQLRepProvider[DatabaseNamesMetaTypes](
    extraContext.cryptProviderProvider,
    sqlizer.exprSqlFactory,
    sqlizer.namespace,
    sqlizer.toProvenance,
    sqlizer.isRollup,
    extraContext.locationSubcolumns,
    physicalTableFor
  ) {
    override def mkStringLiteral(text: String): Doc = {
      // By default, converting a String to Doc replaces the newlines
      // with soft newlines which can be converted into spaces by
      // `group`.  This is a thing we _definitely_ don't want, so
      // instead replace those newlines with hard line breaks, and
      // un-nest lines by the current nesting level so the linebreak
      // doesn't introduce any indentation.
      val escapedText = extraContext.escapeString(text)
        .split("\n", -1)
        .toSeq
        .map(Doc(_))
        .concatWith { (a: Doc, b: Doc) =>
        a ++ Doc.hardline ++ b
      }
      val unindented = Doc.nesting { depth => escapedText.nest(-depth) }
      d"'" ++ unindented ++ d"'"
    }
  }
)
