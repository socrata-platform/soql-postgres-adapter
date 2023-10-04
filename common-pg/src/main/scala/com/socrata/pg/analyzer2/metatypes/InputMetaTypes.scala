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
import com.socrata.soql.functions.{MonomorphicFunction, SoQLFunctionInfo, SoQLTypeInfo}

import com.socrata.pg.analyzer2.SoQLValueDebugHelper
import com.socrata.pg.analyzer2.{metatypes => mt}
import com.socrata.pg.store.PGSecondaryUniverse

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

  class CopyCache(pgu: PGSecondaryUniverse[InputMetaTypes#ColumnType, InputMetaTypes#ColumnValue]) extends mt.CopyCache[InputMetaTypes] {
    private val map = new scm.HashMap[DatabaseTableName, (CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])]

    override def allCopies = map.valuesIterator.map(_._1).toVector

    // returns None if the database table name is unknown to this secondary
    def apply(dtn: DatabaseTableName): Option[(CopyInfo, OrderedMap[UserColumnId, ColumnInfo[CT]])] =
      map.get(dtn).orElse {
        val DatabaseTableName((internalName, Stage(lifecycleStage))) = dtn
        val reader = pgu.datasetMapReader
        for {
          dsInfo <- reader.datasetInfoByInternalName(internalName)
          copy <- lifecycleStage.toLowerCase match { // ughghhhhg Stage should totally just be a LifecycleStage already
            case "published" => reader.published(dsInfo)
            case "unpublished" => reader.unpublished(dsInfo)
            case _ => None
          }
        } yield {
          val schemaBySystemId = pgu.datasetMapReader.schema(copy)
          val schemaByUserId = OrderedMap() ++ schemaBySystemId.values.toSeq.sortBy(_.systemId).map { colInfo => colInfo.userColumnId -> colInfo }
          map += dtn -> ((copy, schemaByUserId))
          (copy, schemaByUserId)
        }
      }

    def mostRecentlyModifiedAt = {
      val it = map.valuesIterator.map(_._1.lastModified)
      if(it.hasNext) Some(it.maxBy(_.getMillis))
      else None
    }

    // All the dataVersions of all the datasets involved, in some
    // arbitrary but consistent order.
    def orderedVersions =
      map.to.toSeq.sortBy { case (DatabaseTableName((DatasetInternalName(instance, dsId), Stage(stage))), (copyInfo, _)) =>
        (instance, dsId.underlying, stage, copyInfo.copyNumber)
      }.map { case (_dtn, (copyInfo, _colInfos)) => copyInfo.dataVersion }
  }
}
