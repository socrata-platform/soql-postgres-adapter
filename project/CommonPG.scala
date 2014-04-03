import com.rojoma.json.util.JsonUtil._
import com.rojoma.simplearm.util._
import sbt._
import Keys._

import Dependencies._

object CommonPG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings() ++ Seq(
    libraryDependencies ++= Seq(
      commonsCodec,
      commonsIo,
      jodaConvert,
      jodaTime,
      liquibaseCore,
      liquibasePlugin,
      socrataUtil,
      soqlStdlib,
      secondarylib,
      coordinatorlib,
      typesafeConfig,
      slf4j,
      typesafeScalaLogging,
      rojomaJson
    )
  )

  def genVersion(resourceManaged: File, name: String, version: String, scalaVersion: String): Seq[File] = {
    val file = resourceManaged / (name + ".json")

    val revision = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.split("\n")(0)

    val result = Map(
      "service" -> name,
      "version" -> version,
      "revision" -> revision,
      "scala" -> scalaVersion
    ) ++ sys.env.get("BUILD_TAG").map("build" -> _)

    resourceManaged.mkdirs()
    for {
      stream <- managed(new java.io.FileOutputStream(file))
      w <- managed(new java.io.OutputStreamWriter(stream, "UTF-8"))
    } {
      writeJson(w, result, pretty = true)
      w.write("\n")
    }

    Seq(file)
  }
}


