package com.socrata.pg

import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.matcher.{PObject, Variable}
import com.rojoma.json.ast.{JValue, JString, JObject}
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.metadata.Schema
import com.socrata.datacoordinator.util.collection.UserColumnIdMap
import com.socrata.soql.environment.TypeName

object Schema {

  implicit object SchemaCodec extends JsonCodec[Schema] {
    private implicit val schemaProperCodec = new JsonCodec[UserColumnIdMap[TypeName]] {
      def encode(schema: UserColumnIdMap[TypeName]) =  {
        val map = schema.foldLeft(Map.empty[String, JValue]) { (acc, item) =>
          item match {
            case (k, v) =>
              acc + (k.underlying -> JString(v.name))
          }
        }
        JObject(map)
      }
      def decode(x: JValue) =  {
        val mapOpt = JsonCodec[Map[String, String]].decode(x).map {
          m => m.map { case (k, v) => new UserColumnId(k) -> TypeName(v)
        }}
        mapOpt.map(UserColumnIdMap(_))
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

    def encode(schemaObj: Schema) = {
      PSchema.generate(
        hashVar := schemaObj.hash,
        schemaVar := schemaObj.schema,
        pkVar := schemaObj.pk.underlying,
        localeVar := schemaObj.locale)
    }

    def decode(x: JValue) = PSchema.matches(x) map { results =>
      new Schema(hashVar(results), schemaVar(results), new UserColumnId(pkVar(results)), localeVar(results))
    }
  }
}