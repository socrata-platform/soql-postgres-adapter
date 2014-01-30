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
      socrataUtil,
      soqlStdlib,
      secondarylib,
      coordinatorlib,
      typesafeConfig,
      slf4j,
      h2Database
    )
  )
}


