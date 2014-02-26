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

    val cjsonSortedSchema = requestSchema.values.toSeq.sortWith(_.userColumnId.underlying < _.userColumnId.underlying)
    // This maps user column ids to the artifically created column ids consisting of 1,2,3...
    // which is the order returned by sql query
    val userIdToQueryColumnIdMap = colIdMap.keys.foldLeft(Map.empty[UserColumnId, ColumnId]) { (map, cid) =>
      map + (colIdMap(cid).userColumnId -> cid)
    }

    writeSchema(cjsonSortedSchema, writer)
    writer.write("\n }")

    rowData foreach {
      row =>
        val jsonRowValues = cjsonSortedSchema.map { (cinfo: ColumnInfo[SoQLType]) =>
          val rep = jsonReps(cinfo.typ)
          val columnId = userIdToQueryColumnIdMap(cinfo.userColumnId)
          val soqlValue = row(columnId)
          rep.toJValue(soqlValue)
        }
        logger.info("Writing " + JArray(jsonRowValues))
        writer.write("\n,")
        CompactJsonWriter.toWriter(writer, JArray(jsonRowValues))
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
