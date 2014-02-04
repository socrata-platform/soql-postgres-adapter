package com.socrata.pg.store

import com.rojoma.simplearm.util._
import scala.io.{Codec, Source}
import com.socrata.datacoordinator.util.TemplateReplacer
import com.socrata.datacoordinator.truth.sql.DatasetMapLimits

object DatabasePopulator {

  private def load(template: String) = {
    using(getClass.getResourceAsStream(template)) { stream =>
      Source.fromInputStream(stream)(Codec.UTF8).getLines().mkString("\n")
    }
  }

  // By default, the schema loaded will be that which is defined in "secondary_db_schema.sql"
  // which defines the basic collection of table that will be created in the "secondary" database.
  // The template file is expected to be in "store-pg/src/main/resources/com/socrata/pg/store/"
  def createSchema(templateName: String = "secondary_db_schema.template.sql"): String = {
    object StandardDatasetMapLimits extends DatasetMapLimits
    val limits = StandardDatasetMapLimits

    TemplateReplacer(
      load(templateName),
      Map(
        "user_uid_len" -> limits.maximumUserIdLength.toString,
        "user_column_id_len" -> limits.maximumLogicalColumnNameLength.toString,
        "physcol_base_len" -> limits.maximumPhysicalColumnBaseLength.toString,
        "type_name_len" -> limits.maximumTypeNameLength.toString,
        "store_id_len" -> limits.maximumStoreIdLength.toString,
        "table_name_len" -> limits.maximumPhysicalTableNameLength.toString,
        "locale_name_len" -> limits.maximumLocaleNameLength.toString
      )
    )
  }

  def populate(conn: java.sql.Connection) {
    using(conn.createStatement) {
      stmt => stmt.execute(createSchema())
    }
  }

}
