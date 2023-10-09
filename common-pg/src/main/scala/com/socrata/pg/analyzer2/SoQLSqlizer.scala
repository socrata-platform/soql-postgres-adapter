package com.socrata.pg.analyzer2

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.sqlizer._
import com.socrata.datacoordinator.common

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

object ActualSqlizer {
    def choose(sqlizer: common.DbType) = sqlizer match {
    case common.Redshift => (RedshiftSqlizer.apply _)
    case common.Postgres => (PostgresSqlizer.apply _)
  }
}

case class PostgresSqlizer(
  escapeString: String => String,
  cryptProviderProvider: CryptProviderProvider,
  override val systemContext: Map[String, String],
  locationSubcolumns: PostgresSqlizer.LocationSubcolumns
) extends Sqlizer[DatabaseNamesMetaTypes] {
  override val exprSqlFactory = new PostgresExprSqlFactory[DatabaseNamesMetaTypes]

  override val namespace = new PostgresNamespaces

  override val toProvenance = DatabaseNamesMetaTypes.provenanceMapper
  override def isRollup(dtn: DatabaseTableName) = dtn.name.isRollup

  override def mkRepProvider(physicalTableFor: Map[AutoTableLabel, DatabaseTableName]): Rep.Provider[DatabaseNamesMetaTypes] =
    new SoQLRepProviderPostgres[DatabaseNamesMetaTypes](
      cryptProviderProvider,
      exprSqlFactory,
      namespace,
      toProvenance,
      isRollup,
      locationSubcolumns,
      physicalTableFor
    ) {
      override def mkStringLiteral(text: String): Doc = {
        // By default, converting a String to Doc replaces the newlines
        // with soft newlines which can be converted into spaces by
        // `group`.  This is a thing we _definitely_ don't want, so
        // instead replace those newlines with hard line breaks, and
        // un-nest lines by the current nesting level so the linebreak
        // doesn't introduce any indentation.
        val escapedText = escapeString(text)
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

  override val funcallSqlizer = PostgresSqlizer.funcallSqlizer

  override val rewriteSearch = PostgresSqlizer.rewriteSearch
}

case class RedshiftSqlizer(
  escapeString: String => String,
  cryptProviderProvider: CryptProviderProvider,
  override val systemContext: Map[String, String],
  locationSubcolumns: RedshiftSqlizer.LocationSubcolumns
) extends Sqlizer[DatabaseNamesMetaTypes] {
  override val exprSqlFactory = new RedshiftExprSqlFactory[DatabaseNamesMetaTypes]

  override val namespace = new RedshiftNamespaces

  override val toProvenance = DatabaseNamesMetaTypes.provenanceMapper
  override def isRollup(dtn: DatabaseTableName) = dtn.name.isRollup

  override def mkRepProvider(physicalTableFor: Map[AutoTableLabel, DatabaseTableName]): Rep.Provider[DatabaseNamesMetaTypes] =
    new SoQLRepProviderRedshift[DatabaseNamesMetaTypes](
      cryptProviderProvider,
      exprSqlFactory,
      namespace,
      toProvenance,
      isRollup,
      locationSubcolumns,
      physicalTableFor
    ) {
      override def mkStringLiteral(text: String): Doc = {
        // By default, converting a String to Doc replaces the newlines
        // with soft newlines which can be converted into spaces by
        // `group`.  This is a thing we _definitely_ don't want, so
        // instead replace those newlines with hard line breaks, and
        // un-nest lines by the current nesting level so the linebreak
        // doesn't introduce any indentation.
        val escapedText = escapeString(text)
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

  override val funcallSqlizer = RedshiftSqlizer.funcallSqlizer

  override val rewriteSearch = RedshiftSqlizer.rewriteSearch
}

object PostgresSqlizer extends SqlizerUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]
  private val funcallSqlizer = new SoQLFunctionSqlizerPostgres[DatabaseNamesMetaTypes]
  private val rewriteSearch = new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true)
}

object RedshiftSqlizer extends SqlizerUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]
  private val funcallSqlizer = new SoQLFunctionSqlizerRedshift[DatabaseNamesMetaTypes]
  private val rewriteSearch = new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true)
}
