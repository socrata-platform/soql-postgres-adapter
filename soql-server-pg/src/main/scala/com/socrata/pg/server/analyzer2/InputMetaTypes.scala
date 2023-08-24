package com.socrata.pg.server.analyzer2

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.prettyprint.prelude._

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}

import com.socrata.soql.analyzer2._
import com.socrata.soql.environment.Provenance
import com.socrata.soql.serialize._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctionInfo, SoQLTypeInfo}

final class InputMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = (DatasetInternalName, Stage)
  override type DatabaseColumnNameImpl = UserColumnId
}

object InputMetaTypes {
  val typeInfo = SoQLTypeInfo.metaProject[InputMetaTypes]

  val provenanceMapper = new types.ProvenanceMapper[InputMetaTypes] {
    def fromProvenance(prov: Provenance): types.DatabaseTableName[InputMetaTypes] = {
      JsonUtil.parseJson[types.DatabaseTableName[InputMetaTypes]](prov.value) match {
        case Right(result) => result
        case Left(e) => throw new Exception(e.english)
      }
    }

    def toProvenance(dtn: types.DatabaseTableName[InputMetaTypes]): Provenance = {
      Provenance(JsonUtil.renderJson(dtn, pretty=false))
    }
  }

  object DeserializeImplicits {
    implicit object dinDeserialize extends Readable[DatasetInternalName] {
      def readFrom(buffer: ReadBuffer) =
        DatasetInternalName(buffer.read[String]()).getOrElse(fail("invalid dataset internal name"))
    }

    implicit object ucidDeserialize extends Readable[UserColumnId] {
      def readFrom(buffer: ReadBuffer) =
        new UserColumnId(buffer.read[String]())
    }

    implicit object sDeserialize extends Readable[Stage] {
      def readFrom(buffer: ReadBuffer) = Stage(buffer.read[String]())
    }

    implicit val hasType = com.socrata.soql.functions.SoQLTypeInfo.hasType
    implicit val mfDeserialize = MonomorphicFunction.deserialize(SoQLFunctionInfo)
  }

  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
    implicit object hasDocDBTN extends HasDoc[(DatasetInternalName, Stage)] {
      def docOf(cv: (DatasetInternalName, Stage)) = Doc(cv.toString)

    }
    implicit object hasDocDBCN extends HasDoc[UserColumnId] {
      def docOf(cv: UserColumnId) = Doc(cv.toString)
    }
  }
}
