package com.socrata.pg

import com.rojoma.json.v3.codec.JsonDecode.DecodeResult
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.matcher.{PObject, Variable}
import com.rojoma.json.v3.ast.{JValue, JString, JObject}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.{Schema => TruthSchema}
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.socrata.soql.environment.TypeName

object Schema {

  implicit object SchemaCodec extends JsonDecode[TruthSchema] with JsonEncode[TruthSchema] {

    private implicit val schemaProperCodec = new JsonDecode[UserColumnIdMap[TypeName]]
      with JsonEncode[UserColumnIdMap[TypeName]] {
      def encode(schema: UserColumnIdMap[TypeName]): JValue = {
        val map = schema.foldLeft(Map.empty[String, JValue]) { (acc, item) =>
          item match {
            case (k, v) =>
              acc + (k.underlying -> JString(v.name))
          }
        }
        JObject(map)
      }

      def decode(x: JValue): DecodeResult[UserColumnIdMap[TypeName]] =  {
        JsonDecode[Map[String, String]].decode(x) match {
          case Right(m) =>
            val idTypeName = m.map { case (k, v) => new UserColumnId(k) -> TypeName(v)}
            Right(UserColumnIdMap(idTypeName))
          case Left(err) => Left(err)
        }
      }
    }

    private val hashVar = Variable[String]()
    private val schemaVar = Variable[UserColumnIdMap[TypeName]]()
    private val pkVar = Variable[String]()
    private val localeVar = Variable[String]
    private val PSchema = PObject(
      "hash" -> hashVar,
      "schema" -> schemaVar,
      "pk" -> pkVar,
      "locale" -> localeVar
    )

    def encode(schemaObj: TruthSchema) = {
      PSchema.generate(
        hashVar := schemaObj.hash,
        schemaVar := schemaObj.schema,
        pkVar := schemaObj.pk.underlying,
        localeVar := schemaObj.locale)
    }

    def decode(x: JValue) = PSchema.matches(x) match {
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