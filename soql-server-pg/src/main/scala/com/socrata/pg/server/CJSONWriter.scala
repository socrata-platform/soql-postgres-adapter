package com.socrata.pg.server

import com.rojoma.json.ast._
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.rojoma.simplearm.util._
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, ColumnInfo}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.types._
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.{OutputStreamWriter, Writer}
import javax.servlet.http.HttpServletResponse

/**
 * Writes rows as CJSON
 *  [
 *    {
 *      approximate_row_count: x,
 *      data_version: y,
 *      last_modified: ISODateTime,
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

  val dateTimeFormat = ISODateTimeFormat.dateTime

  def writeCJson(datasetInfo: DatasetInfo,
                 qrySchema: OrderedMap[com.socrata.datacoordinator.id.ColumnId, com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]],
                 rowData:CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]],
                 reqRowCount: Boolean,
                 givenRowCount: Option[Long],
                 dataVersion: Long,
                 lastModified: DateTime,
                 locale: String = "en_US") = (r:HttpServletResponse) => {

    r.setContentType("application/json")
    r.setCharacterEncoding(scala.io.Codec.UTF8.name)
    val os = r.getOutputStream
    val jsonReps = PostgresUniverseCommon.jsonReps(datasetInfo)

    val (rowCount, rows) = givenRowCount match {
      case None if reqRowCount =>
        logger.warn("FIXME: I am brute forcing row count!")
        val consumedRowData = rowData.toSeq
        val it = consumedRowData.iterator
        val cit = new CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]] {
          def next() = { it.next() }
          def hasNext = it.hasNext
          def close() = { }
        }
        (Some(consumedRowData.size.toLong), cit)
      case rc =>
        (rc, rowData)
    }

    using(new OutputStreamWriter(os)) { (writer: OutputStreamWriter) =>
      writer.write("[{")
      rowCount.map { rc =>
        writer.write("\"approximate_row_count\": %s\n,".format(JNumber(rc).toString))
      }
      writer.write("\"data_version\":%d\n,".format(dataVersion))
      writer.write("\"last_modified\":\"%s\"\n,".format(dateTimeFormat.print(lastModified)))
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

      for (row <- rows) {
        assert(row.size == cids.length)
        var result = new Array[JValue](row.size)

        for (i <- 0 until result.length) {
          result(i) = reps(i).toJValue(row(cids(i)))
        }
        writer.write("\n,")
        CompactJsonWriter.toWriter(writer, JArray(result))
      }
      writer.write("\n]\n")
      writer.flush()
    }
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
