package com.socrata.pg.server.analyzer2

import scala.reflect.ClassTag

import com.socrata.prettyprint.prelude._
import com.socrata.soql.analyzer2._
import com.socrata.soql.types.obfuscation.CryptProvider

import com.socrata.pg.analyzer2.{Sqlizer, SqlNamespaces, SoQLRepProvider, SoQLFunctionSqlizer, Rep, CryptProviderProvider, ResultExtractor, SoQLRewriteSearch, SqlizerUniverse}

class ActualSqlizer(
  escapeString: String => String,
  cryptProviderProvider: CryptProviderProvider,
  override val systemContext: Map[String, String],
  locationSubcolumns: ActualSqlizer.LocationSubcolumns,
  physicalTableFor: Map[AutoTableLabel, types.DatabaseTableName[DatabaseNamesMetaTypes]]
) extends Sqlizer[DatabaseNamesMetaTypes] {
  override val namespace = new SqlNamespaces {
    def databaseTableName(dtn: DatabaseTableName) = {
      val DatabaseTableName(dataTableName) = dtn
      Doc(dataTableName)
    }
    def databaseColumnBase(dcn: DatabaseColumnName) = {
      val DatabaseColumnName(physicalColumnBase) = dcn
      Doc(physicalColumnBase)
    }
  }

  override val repFor: Rep.Provider[DatabaseNamesMetaTypes] = new SoQLRepProvider[DatabaseNamesMetaTypes](cryptProviderProvider, namespace, locationSubcolumns, physicalTableFor) {
    def mkStringLiteral(text: String): Doc =
      d"'" ++ Doc(escapeString(text)) ++ d"'"
  }

  override val funcallSqlizer = ActualSqlizer.funcallSqlizer

  override val rewriteSearch = ActualSqlizer.rewriteSearch
}

object ActualSqlizer extends SqlizerUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]
  private val funcallSqlizer = new SoQLFunctionSqlizer[DatabaseNamesMetaTypes]
  private val rewriteSearch = new SoQLRewriteSearch[DatabaseNamesMetaTypes](searchBeforeQuery = true)
}
