package com.socrata.pg.server

import com.socrata.datacoordinator.truth.metadata.{SchemaField, DatasetCopyContext, DatasetInfo}
import com.socrata.datacoordinator.Row
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.id.{UserColumnId, ColumnId}
import com.rojoma.json.ast._
import com.socrata.http.server.util.StrongEntityTag
import java.nio.charset.StandardCharsets
import com.rojoma.json.ast.JString

/**
 * Writes rows as CJSON
 */
class CJSONWriter {
  /*
  def toCJSON(copyCtx:DatasetCopyContext[SoQLType], it:Iterator[Row[SoQLValue]]):(StrongEntityTag, Array[SchemaField], Option[UserColumnId], String, Long, Array[JValue]) = {
    val jsonReps = PostgresUniverseCommon.jsonReps(copyCtx.datasetInfo)
      val jsonSchema = copyCtx.schema.mapValuesStrict { ci => jsonReps(ci.typ)}
    val unwrappedCids = copyCtx.schema.values.toSeq.filter { ci => jsonSchema.contains(ci.systemId) }.sortBy(_.userColumnId).map(_.systemId.underlying).toArray
    val pkColName = copyCtx.pkCol.map(_.userColumnId)
    val orderedSchema = unwrappedCids.map { cidRaw =>
      val col = copyCtx.schema(new ColumnId(cidRaw))
      SchemaField(col.userColumnId, col.typ.name.name)
    } map {
      ci => JObject(Map(ci.userColumnId.toString() -> JString(ci.typ)))
    }
    val entityTag = StrongEntityTag(copyCtx.copyInfo.dataVersion.toString.getBytes(StandardCharsets.UTF_8))

    val (count:Long, jsonRows:Array[JValue]) = it.map { row =>
      val arr = new Array[JValue](unwrappedCids.length)
      var i = 0
      while(i != unwrappedCids.length) {
        val cid = new ColumnId(unwrappedCids(i))
        val rep = jsonSchema(cid)
        arr(i) = rep.toJValue(row(cid))
        i += 1
      }
      (i, arr)
    }
    JArray(JString(entityTag.toString()),
    JArray(orderedSchema),
    JString(pkColName.),
    JString(copyCtx.datasetInfo.localeName),
    JNumber(count),
    JArray(jsonRows)))
  }
  */
}
