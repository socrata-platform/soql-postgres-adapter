package com.socrata.pg.server.analyzer2

import scala.concurrent.duration._

import java.io.InputStream

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}

import com.socrata.soql.analyzer2.{SoQLAnalysis, LabelUniverse}
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.serialize.{ReadBuffer, Readable}
import com.socrata.soql.sql.Debug

import com.socrata.pg.analyzer2.metatypes.{InputMetaTypes, Stage}

object Deserializer extends LabelUniverse[InputMetaTypes] {
  case class AuxTableData(
    locationSubcolumns: Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]],
    sfResourceName: String,
    truthDataVersion: Long
  )
  object AuxTableData {
    implicit def deserialize(
      implicit ev1: Readable[DatasetInternalName],
      ev2: Readable[UserColumnId],
      ev3: Readable[Stage]
    ) = new Readable[AuxTableData] {
      def readFrom(buffer: ReadBuffer): AuxTableData = {
        buffer.read[Int]() match {
          case 0 =>
            AuxTableData(
              buffer.read[Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]](),
              buffer.read[String](),
              buffer.read[Long]()
            )
          case other =>
            fail(s"Unknown aux table data version $other")
        }
      }
    }
  }

  case class Request(
    analysis: SoQLAnalysis[InputMetaTypes],
    auxTableData: Map[DatabaseTableName, AuxTableData],
    context: Map[String, String],
    passes: Seq[Seq[Pass]],
    allowRollups: Boolean,
    debug: Option[Debug],
    queryTimeout: Option[FiniteDuration]
  )
  object Request {
    implicit def deserialize(
      implicit ev1: Readable[SoQLAnalysis[InputMetaTypes]],
      ev2: Readable[DatasetInternalName],
      ev3: Readable[UserColumnId],
      ev4: Readable[Stage],
    ) = new Readable[Request] {
      def readFrom(buffer: ReadBuffer): Request = {
        buffer.read[Int]() match {
          case 1 =>
            Request(
              buffer.read[SoQLAnalysis[InputMetaTypes]](),
              buffer.read[Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]]().mapValues { locs =>
                AuxTableData(locs, "", -1)
              },
              buffer.read[Map[String, String]](),
              buffer.read[Seq[Seq[Pass]]](),
              buffer.read[Boolean](),
              buffer.read[Option[Debug]](),
              buffer.read[Option[Long]]().map(_.milliseconds)
            )
          case 2 =>
            Request(
              buffer.read[SoQLAnalysis[InputMetaTypes]](),
              buffer.read[Map[DatabaseTableName, AuxTableData]](),
              buffer.read[Map[String, String]](),
              buffer.read[Seq[Seq[Pass]]](),
              buffer.read[Boolean](),
              buffer.read[Option[Debug]](),
              buffer.read[Option[Long]]().map(_.milliseconds)
            )
          case other =>
            fail(s"Unknown request version $other")
        }
      }
    }
  }

  def apply(inputStream: InputStream): Request = {
    import InputMetaTypes.DeserializeImplicits._

    ReadBuffer.read[Request](inputStream)
  }
}
