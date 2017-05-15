package com.socrata.pg

import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.matcher.{PObject, Variable}
import com.rojoma.json.v3.ast._
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.{SchemaWithFieldName => TruthSchema}
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.socrata.soql.environment.{ColumnName, TypeName}

object ExtendedSchema {
  implicit object SchemaCodec extends JsonDecode[TruthSchema] with JsonEncode[TruthSchema] {

    private implicit val schemaProperCodec = new JsonDecode[UserColumnIdMap[(TypeName, Option[ColumnName])]]
      with JsonEncode[UserColumnIdMap[(TypeName, Option[ColumnName])]] {
      def encode(schema: UserColumnIdMap[(TypeName, Option[ColumnName])]): JValue = {
        val map = schema.foldLeft(Map.empty[String, JValue]) { (acc, item) =>
          item match {
            case (k, v) =>
              val fieldName = v._2.map(x => JString(x.name)).getOrElse(JNull)
              acc + (k.underlying -> JArray(Seq(JString(v._1.name), fieldName)))
          }
        }
        JObject(map)
      }

      def decode(x: JValue): DecodeResult[UserColumnIdMap[(TypeName, Option[ColumnName])]] =  {
        JsonDecode[Map[String, JArray]].decode(x) match {
          case Right(m) =>
            val idTypeName = m.map { case (k, Seq(JString(typename), fieldNameOpt)) =>
              val fieldName = fieldNameOpt match {
                case JString(x) => Some(ColumnName(x))
                case _ => None
              }
              Tuple2(new UserColumnId(k), (TypeName(typename), fieldName))
            }
            Right(UserColumnIdMap(idTypeName))
          case Left(err) => Left(err)
        }
      }
    }

    private val hashVar = Variable[String]()
    private val schemaVar = Variable[UserColumnIdMap[(TypeName, Option[ColumnName])]]()
    private val pkVar = Variable[String]()
    private val localeVar = Variable[String]
    private val PSchema = PObject(
      "hash" -> hashVar,
      "schema" -> schemaVar,
      "pk" -> pkVar,
      "locale" -> localeVar
    )

    def encode(schemaObj: TruthSchema): JValue = {
      PSchema.generate(
        hashVar := schemaObj.hash,
        schemaVar := schemaObj.schema,
        pkVar := schemaObj.pk.underlying,
        localeVar := schemaObj.locale)
    }

    def decode(x: JValue): DecodeResult[TruthSchema] = PSchema.matches(x) match {
      case Right(results) =>
        Right(new TruthSchema(
          hashVar(results),
          schemaVar(results),
          new UserColumnId(pkVar(results)),
          localeVar(results)))
      case Left(err) => Left(err)
    }
  }
}
