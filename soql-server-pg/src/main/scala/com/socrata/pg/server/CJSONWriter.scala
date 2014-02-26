package com.socrata.pg.server

import com.rojoma.json.ast._
import java.io.{OutputStreamWriter, BufferedWriter, Writer}
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.soql.types._
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, CopyInfo, DatasetCopyContext, ColumnInfo}
import com.rojoma.simplearm._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.soql.SoQLAnalysis
import com.socrata.pg.store.PostgresUniverseCommon
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import com.socrata.datacoordinator.truth.DatasetReader._
import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import javax.servlet.http.HttpServletResponse
import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.soql.collection.{OrderedMap, OrderedSet}

/**
 * Writes rows as CJSON
 *  [
 *    {
 *      approximate_row_count: x,
 *      locale: en,
 *      "pk"
 *      schema: [
 *                  {name -> type},
 *                  {name2 -> type}
 *              ],
 *    },
 *    [row 1],
 *    [row 2],
 *    [row 3],
 *
 */
object CJSONWriter {
  val logger: Logger =
    Logger(LoggerFactory getLogger getClass.getName)

  def writeCJson(datasetInfo: DatasetInfo,
                 qrySchema: OrderedMap[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]],
                 rowData:CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]],
                 rowCount: Option[Long],
                 locale: String = "en_US") = (r:HttpServletResponse) => {
    r.setContentType("application/json")
    r.setCharacterEncoding("utf-8")
    val os = r.getOutputStream
    val writer:Writer = new OutputStreamWriter(os)
    val jsonReps = PostgresUniverseCommon.jsonReps(datasetInfo)

    writer.write("[{")
    rowCount.map { rc =>
      writer.write("\"approximate_row_count\": %s\n,".format(JNumber(rc).toString))
    }
    writer.write("\"locale\":\"%s\"".format(locale))

    val cjsonSortedSchema = qrySchema.values.toSeq.sortWith(_.userColumnId.underlying < _.userColumnId.underlying)
    val qryColumnIdToUserColumnIdMap = qrySchema.foldLeft(Map.empty[UserColumnId, ColumnId]) { (map, entry) =>
      val (cid, cInfo) = entry
      map + (cInfo.userColumnId -> cid)
    }
    val reps = cjsonSortedSchema.map { cinfo => jsonReps(cinfo.typ) }.toArray
    val cids = cjsonSortedSchema.map { cinfo => qryColumnIdToUserColumnIdMap(cinfo.userColumnId) }.toArray

    writeSchema(cjsonSortedSchema, writer)
    writer.write("\n }")

    for (row <- rowData) {
      assert(row.size == cids.length)
      var i = 0
      var result = new Array[JValue](row.size)
      while(i != result.length) {
        result(i) = reps(i).toJValue(row(cids(i)))
        i += 1
      }
      writer.write("\n,")
      CompactJsonWriter.toWriter(writer, JArray(result))// JArray(jsonRowValues))
    }
    writer.write("\n]\n")
    writer.flush()
    writer.close()
  }

  private def writeSchema(cjsonSortedSchema:Seq[ColumnInfo[SoQLType]], writer: Writer) {
    val sel = cjsonSortedSchema.map { colInfo => Field(colInfo.userColumnId.underlying, colInfo.typ.toString()) }.toArray
    writer.write("\n ,\"schema\":")
    JsonUtil.writeJson(writer, sel)
  }

  private case class Field(c: String, t: String)

  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  private type PGJ2CJ = JValue => JValue

}
