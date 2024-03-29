package com.socrata.pg.server

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.rojoma.simplearm.v2._
import com.socrata.datacoordinator.Row
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.metadata.{DatasetInfo, ColumnInfo}
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.pg.store.PostgresUniverseCommon
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.types._
import com.typesafe.scalalogging.Logger
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import java.io.{OutputStreamWriter, Writer}
import javax.servlet.http.HttpServletResponse

final abstract class CJSONWriter

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
  val logger = Logger[CJSONWriter]

  val dateTimeFormat = ISODateTimeFormat.dateTime
  val utf8EncodingName = scala.io.Codec.UTF8.name

  def writeCJson(// scalastyle:ignore method.length parameter.number
                 datasetInfo: DatasetInfo,
                 qrySchema: OrderedMap[ColumnId, ColumnInfo[SoQLType]],
                 rowData:CloseableIterator[Row[SoQLValue]],
                 reqRowCount: Boolean,
                 givenRowCount: Option[Long],
                 dataVersion: Long,
                 lastModified: DateTime,
                 obfuscateId: Boolean,
                 locale: String = "en_US"): HttpServletResponse => Unit = (r: HttpServletResponse) => {
    r.setContentType("application/json")
    r.setCharacterEncoding(utf8EncodingName)
    val os = r.getOutputStream
    val jsonReps = PostgresUniverseCommon.jsonReps(datasetInfo, obfuscateId)

    val (rowCount, rows) = givenRowCount match {
      case None if reqRowCount =>
        logger.warn("FIXME: I am brute forcing row count!")
        val consumedRowData = rowData.toSeq
        val it = consumedRowData.iterator
        val cit = new CloseableIterator[Row[SoQLValue]] {
          def next() = { it.next() }
          def hasNext = it.hasNext
          def close() = { }
        }
        (Some(consumedRowData.size.toLong), cit)
      case rc: Option[Long] => (rc, rowData)
    }

    using(new OutputStreamWriter(os, utf8EncodingName)) { (writer: OutputStreamWriter) =>
      // CJSON in the original definition is sorted by field name.
      // But here cjson is used as an intermediary tool to serve csv, json etc where following SELECT order is human friendly.
      // Upstream service like soda-fountain may re-sort cjson output.
      val cjsonSortedSchema = qrySchema.values.toSeq
      val qryColumnIdToUserColumnIdMap = qrySchema.foldLeft(Map.empty[UserColumnId, ColumnId]) { (map, entry) =>
        val (cid, cInfo) = entry
        map + (cInfo.userColumnId -> cid)
      }
      val repsResult = try {
        Right(cjsonSortedSchema.map { cinfo => jsonReps(cinfo.typ) }.toArray)
      } catch {
        case ex: NoSuchElementException =>
          r.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          JsonUtil.writeJson(
            writer,
            QCRequestError(s"""Datatype ${ex.getMessage.split(" ").last} does not support output""")
          )
          Left(ex)
      }

      repsResult match {
        case Right(reps) =>

          val cids = cjsonSortedSchema.map { cinfo => qryColumnIdToUserColumnIdMap(cinfo.userColumnId) }.toArray

          writer.write("[{")
          rowCount.foreach { rc =>
            writer.write("\"approximate_row_count\": %s\n,".format(JNumber(rc).toString()))
          }
          writer.write("\"data_version\":%d\n,".format(dataVersion))
          writer.write("\"last_modified\":\"%s\"\n,".format(dateTimeFormat.print(lastModified)))
          writer.write("\"locale\":\"%s\"".format(locale))

          writeSchema(cjsonSortedSchema, writer)
          writer.write("\n }")

          for {row <- rows} {
            assert(row.size == cids.length)
            val result = new Array[JValue](row.size)

            for (i <- 0 until result.length) {
              result(i) = reps(i).toJValue(row(cids(i)))
            }
            writer.write("\n,")
            CompactJsonWriter.toWriter(writer, JArray(result))
          }
          writer.write("\n]\n")
        case Left(_) =>
      }
      writer.flush()
    }
  }

  private case class Field(c: String, t: String)

  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  private def writeSchema(cjsonSortedSchema:Seq[ColumnInfo[SoQLType]], writer: Writer): Unit = {
    val sel = cjsonSortedSchema.map {
      colInfo => Field(colInfo.userColumnId.underlying, colInfo.typ.toString())
    }.toArray
    writer.write("\n ,\"schema\":")
    JsonUtil.writeJson(writer, sel)
  }

  private type PGJ2CJ = JValue => JValue
}
