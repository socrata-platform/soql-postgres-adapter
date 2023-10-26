package com.socrata.pg.analyzer2.metatypes

import scala.collection.{mutable => scm}

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.prettyprint.prelude._

import com.socrata.datacoordinator.truth.metadata.{CopyInfo, ColumnInfo}
import com.socrata.datacoordinator.id.{DatasetInternalName, UserColumnId}

import com.socrata.soql.analyzer2._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.Provenance
import com.socrata.soql.serialize._
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctionInfo, SoQLTypeInfo2}

import com.socrata.pg.analyzer2.SoQLValueDebugHelper
import com.socrata.pg.analyzer2.{metatypes => mt}
import com.socrata.pg.store.PGSecondaryUniverse

final class RollupMetaTypes extends MetaTypes {
  override type ResourceNameScope = Int
  override type ColumnType = SoQLType
  override type ColumnValue = SoQLValue
  override type DatabaseTableNameImpl = DatasetInternalName
  override type DatabaseColumnNameImpl = UserColumnId
}

object RollupMetaTypes {
  val provenanceMapper = new types.ProvenanceMapper[RollupMetaTypes] {
    def fromProvenance(prov: Provenance): types.DatabaseTableName[RollupMetaTypes] = {
      JsonUtil.parseJson[types.DatabaseTableName[RollupMetaTypes]](prov.value) match {
        case Right(result) => result
        case Left(e) => throw new Exception(e.english)
      }
    }

    def toProvenance(dtn: types.DatabaseTableName[RollupMetaTypes]): Provenance = {
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

    implicit val hasType = com.socrata.soql.functions.SoQLTypeInfo.hasType
    implicit val mfDeserialize = MonomorphicFunction.deserialize(SoQLFunctionInfo)
  }

  object DebugHelper extends SoQLValueDebugHelper { // Various implicits to make things printable
    implicit object hasDocDBTN extends HasDoc[DatasetInternalName] {
      def docOf(cv: DatasetInternalName) = Doc(cv.toString)

    }
    implicit object hasDocDBCN extends HasDoc[UserColumnId] {
      def docOf(cv: UserColumnId) = Doc(cv.toString)
    }
  }

  class CopyCache(
    pgu: PGSecondaryUniverse[RollupMetaTypes#ColumnType, RollupMetaTypes#ColumnValue],
    myCopy: CopyInfo
  ) extends mt.CopyCache[RollupMetaTypes] {
    private val map = new scm.HashMap[DatabaseTableName, (CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])]

    override def allCopies = map.valuesIterator.map(_._1).toVector

    // returns None if the database table name is unknown to this secondary
    def apply(dtn: DatabaseTableName): Option[(CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])] =
      map.get(dtn).orElse {
        val DatabaseTableName(internalName) = dtn
        val reader = pgu.datasetMapReader
        for {
          dsInfo <- reader.datasetInfoByInternalName(internalName)
          copy <- if(dsInfo.systemId == myCopy.datasetInfo.systemId) {
            Some(myCopy)
          } else {
            reader.published(dsInfo)
          }
        } yield {
          val schemaBySystemId = pgu.datasetMapReader.schema(copy)
          val schemaByUserId = OrderedMap() ++ schemaBySystemId.values.toSeq.sortBy(_.systemId).map { colInfo => colInfo.userColumnId -> colInfo }
          map += dtn -> ((copy, schemaByUserId))
          (copy, schemaByUserId)
        }
      }
  }
}
