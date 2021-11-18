package com.socrata.pg.server

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

import javax.servlet.http.HttpServletResponse


case class QCRequestError(description: String,
                          errorCode: String = HttpServletResponse.SC_BAD_REQUEST.toString,
                          data: JObject = JObject(Map("source" -> JString("soql-server"))))

object QCRequestError {
  implicit val qcRequestErrorCodec = AutomaticJsonCodecBuilder[QCRequestError]
}

