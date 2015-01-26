package com.socrata.pg

import com.rojoma.simplearm.util._
import java.nio.charset.StandardCharsets

object Version {

  def apply(name: String) = for {
    stream <- managed(getClass.getClassLoader.getResourceAsStream(s"$name.json"))
    reader <- managed(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8))
  } yield com.rojoma.json.v3.io.JsonReader.fromReader(reader)
}
