package com.socrata.pg.server

import com.rojoma.json.ast._
import java.io.{OutputStreamWriter, BufferedWriter, Writer}
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.soql.types._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.datacoordinator.truth.metadata.{CopyInfo, DatasetCopyContext, ColumnInfo}
import com.rojoma.simplearm._
import com.socrata.datacoordinator.util.CloseableIterator
import com.socrata.soql.SoQLAnalysis
import com.socrata.pg.store.PostgresUniverseCommon
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import com.socrata.datacoordinator.truth.DatasetReader._
import com.socrata.datacoordinator.truth.sql.RepBasedSqlDatasetContext
import javax.servlet.http.HttpServletResponse

/**
 * Writes rows as CJSON
 *  [
 *    {
 *      approximate_row_count: x,
 *      locale: en,
 *      schema: [
 *                  {name -> type},
 *                  {name2 -> type}
 *              ],
 *    },
 *    [row 1]
 *    [row 2]
 *    [row 3]
 *
 */
object CJSONWriter {
  val logger: Logger =
    Logger(LoggerFactory getLogger getClass.getName)

  def writeCJson(analysis:SoQLAnalysis[UserColumnId, SoQLType],
                 copyCtx:DatasetCopyContext[SoQLType],
                 colIdMap:ColumnIdMap[ColumnInfo[SoQLType]],
                 rowData:CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]],
                 rowCount: Option[Long],
                 locale: String = "en_US") = (r:HttpServletResponse) => {
    r.setStatus(200)
    r.setContentType("application/json; charset=utf-8")
    val writer:Writer = new OutputStreamWriter(r.getOutputStream)
    logger.info("Writing to CJSON!")
    val jsonReps = PostgresUniverseCommon.jsonReps(copyCtx.datasetInfo)
    //val jsonSchema = copyCtx.schema.mapValuesStrict { ci => jsonReps(ci.typ)}

    writer.write("[{")
    rowCount.map { rc =>
      writer.write("\"approximate_row_count\": %s\n ,".format(JNumber(rc).toString))
    }
    writer.write("\"locale\":\"%s\"".format(locale))
    writeSchema(analysis, writer)

    writer.write("\n }")

    rowData foreach {
      row => println("PG Query Server: " + row.toString)
        val jsonRowValues: Seq[JValue] = colIdMap.foldLeft(Seq.empty[JValue]) {
          (acc, colIdTyp) =>
            val id = colIdTyp._1
            val rep = jsonReps(colIdTyp._2.typ)
            //val rep = jsonSchema(colIdTyp)
            if (row.contains(id)) {
              val cjValue: JValue = rep.toJValue(row(id))
              acc :+ cjValue
            } else {
              acc
            }
        }
        logger.info("Writing " + JArray(jsonRowValues))
        writer.write("\n,")
        CompactJsonWriter.toWriter(writer, JArray(jsonRowValues))
    }
    writer.write("\n]\n")
    writer.flush()
    writer.close()

  }

  private def writeSchema(analysis:SoQLAnalysis[UserColumnId, SoQLType], writer: Writer) {
    val sel = analysis.selection.map { case (k, v) => Field(k.name, v.typ.toString) }.toArray
    val sortedSel = sel.sortWith( _.c < _.c)
    writer.write("\n ,\"schema\":")
    JsonUtil.writeJson(writer, sortedSel)
  }

  private case class Field(c: String, t: String)

  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  private type PGJ2CJ = JValue => JValue

}
