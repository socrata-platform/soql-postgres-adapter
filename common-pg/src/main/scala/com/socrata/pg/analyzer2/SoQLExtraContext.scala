package com.socrata.pg.analyzer2

import org.joda.time.DateTime

import com.socrata.soql.analyzer2._
import com.socrata.soql.sqlizer.ExtraContext

import com.socrata.pg.analyzer2.metatypes.DatabaseNamesMetaTypes

class SoQLExtraContext(
  systemContext: Map[String, String],
  val cryptProviderProvider: CryptProviderProvider,
  val locationSubcolumns: SoQLExtraContext.LocationSubcolumns,
  val escapeString: String => String
) extends ExtraContext[SoQLExtraContext.Result] {
  var obfuscatorRequired = false

  private val actualNow = DateTime.now()
  private var nowUsed = false

  def now: DateTime = {
    nowUsed = true
    actualNow
  }

  private var systemContextUsed: SoQLExtraContext.SystemContextUsed =
    SoQLExtraContext.SystemContextUsed.Static(systemContext, Set())

  def systemContextKeyUsed(key: String): Option[String] = {
    systemContextUsed += key
    systemContextUsed.allContext.get(key)
  }

  def nonLiteralSystemContextUsed(): Unit =
    systemContextUsed = systemContextUsed.dynamic

  override def finish() =
    SoQLExtraContext.Result(
      systemContextUsed,
      if(nowUsed) Some(actualNow) else None,
      obfuscatorRequired
    )
}

object SoQLExtraContext extends StatementUniverse[DatabaseNamesMetaTypes] {
  type LocationSubcolumns = Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]

  sealed abstract class SystemContextUsed {
    def +(key: String): SystemContextUsed
    def allContext: Map[String, String]
    def relevantEtagContext: Map[String, String]
    def dynamicContext: Map[String, String]
    def dynamic: SystemContextUsed.Dynamic
  }
  object SystemContextUsed {
    case class Dynamic(allContext: Map[String, String]) extends SystemContextUsed {
      override def +(key: String) = this
      override def relevantEtagContext = allContext
      override def dynamicContext = allContext
      override def dynamic = this
    }
    case class Static(allContext: Map[String, String], keys: Set[String]) extends SystemContextUsed {
      override def +(key: String) = Static(allContext, keys + key)
      override def relevantEtagContext = allContext.filterKeys(keys)
      override def dynamicContext = Map.empty
      override def dynamic = Dynamic(allContext)
    }
  }

  case class Result(systemContextUsed: SystemContextUsed, now: Option[DateTime], obfuscatorRequired: Boolean)
}
