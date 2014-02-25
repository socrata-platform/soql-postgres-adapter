package com.socrata.pg.server

import com.rojoma.json.ast._
import java.io.{OutputStreamWriter, BufferedWriter, Writer}
import com.rojoma.json.io.CompactJsonWriter
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.soql.types._
import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
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
import com.socrata.datacoordinator.truth.DatasetContext
import com.socrata.soql.collection.OrderedSet

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

  def writeCJson(baseSchema:ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]],
                 copyCtx:DatasetCopyContext[SoQLType],
                 colIdMap:ColumnIdMap[ColumnInfo[SoQLType]],
                 rowData:CloseableIterator[com.socrata.datacoordinator.Row[SoQLValue]],
                 systemToUserColumnMap:Map[ColumnId,UserColumnId],
                 requestColumns:Set[UserColumnId],
                 rowCount: Option[Long],
                 locale: String = "en_US") = (r:HttpServletResponse) => {
    r.setContentType("application/json")
    r.setCharacterEncoding("utf-8")
    val os = r.getOutputStream
    val writer:Writer = new OutputStreamWriter(os)
    val jsonReps = PostgresUniverseCommon.jsonReps(copyCtx.datasetInfo)
    //val jsonSchema = baseSchema.mapValuesStrict { ci => jsonReps(ci.typ)}

    writer.write("[{")
    rowCount.map { rc =>
      writer.write("\"approximate_row_count\": %s\n,".format(JNumber(rc).toString))
    }
    writer.write("\"locale\":\"%s\"".format(locale))

    // We need the user names; but only the ones for the fields we request
    logger.info("Request Columns: " + requestColumns)
    val requestSchema = baseSchema filter {
      (id, info) => systemToUserColumnMap.get(info.systemId) match {
          case Some(cId:UserColumnId) => requestColumns.contains(cId)
          case None => false
        }
    }
    writeSchema(requestSchema, writer)
    writer.write("\n }")

    rowData foreach {
      row =>
        val jsonRowValues: Seq[JValue] = colIdMap.foldLeft(Seq.empty[JValue]) {
          (acc, colIdTyp) =>
            val id = colIdTyp._1
            val rep = jsonReps(colIdTyp._2.typ)
            //val rep = jsonSchema(colIdTyp._1)
            if (row.contains(id)) {
              val cjValue: JValue = rep.toJValue(row(id))
              acc :+ cjValue
            } else {
              logger.warn("Unable to find column " + id + " in row " + row)
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

  private def writeSchema(schema:ColumnIdMap[com.socrata.datacoordinator.truth.metadata.ColumnInfo[SoQLType]], writer: Writer) {
    val sel = schema.values.map { colInfo => Field(colInfo.userColumnId.underlying, colInfo.typ.toString()) }.toArray
    val sortedSel = sel.sortWith( _.c < _.c)
    writer.write("\n ,\"schema\":")
    JsonUtil.writeJson(writer, sortedSel)
  }

  private case class Field(c: String, t: String)

  private implicit val fieldCodec = AutomaticJsonCodecBuilder[Field]

  private type PGJ2CJ = JValue => JValue

}
