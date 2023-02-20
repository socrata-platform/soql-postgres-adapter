package com.socrata.pg.server.analyzer2

import java.io.InputStream

import com.socrata.soql.analyzer2.SoQLAnalysis
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.serialize.{ReadBuffer, Readable}

object Deserializer extends {
  case class Request(
    analysis: SoQLAnalysis[InputMetaTypes],
    context: Map[String, String],
    passes: Seq[Seq[Pass]]
  )
  object Request {
    implicit def deserialize(implicit ev: Readable[SoQLAnalysis[InputMetaTypes]]) = new Readable[Request] {
      def readFrom(buffer: ReadBuffer): Request = {
        buffer.read[Int]() match {
          case 0 =>
            Request(
              buffer.read[SoQLAnalysis[InputMetaTypes]](),
              buffer.read[Map[String, String]](),
              buffer.read[Seq[Seq[Pass]]]()
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
