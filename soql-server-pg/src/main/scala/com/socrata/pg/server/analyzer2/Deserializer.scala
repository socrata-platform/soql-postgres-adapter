package com.socrata.pg.server.analyzer2

import java.io.InputStream

import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}

import com.socrata.soql.analyzer2.{SoQLAnalysis, LabelUniverse}
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.serialize.{ReadBuffer, Readable}
import com.socrata.soql.sql.Debug

object Deserializer extends LabelUniverse[InputMetaTypes] {
  case class Request(
    analysis: SoQLAnalysis[InputMetaTypes],
    locationSubcolumns: Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]],
    context: Map[String, String],
    passes: Seq[Seq[Pass]],
    debug: Option[Debug]
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
          case 0 =>
            Request(
              buffer.read[SoQLAnalysis[InputMetaTypes]](),
              buffer.read[Map[DatabaseTableName, Map[DatabaseColumnName, Seq[Option[DatabaseColumnName]]]]](),
              buffer.read[Map[String, String]](),
              buffer.read[Seq[Seq[Pass]]](),
              buffer.read[Option[Debug]]()
            )
          case other =>
            throw new Exception(s"Unknown request version $other")
        }
      }
    }
  }

  def apply(inputStream: InputStream): Request = {
    import InputMetaTypes.DeserializeImplicits._

    ReadBuffer.read[Request](inputStream)
  }
}