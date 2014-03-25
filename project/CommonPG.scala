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
}


