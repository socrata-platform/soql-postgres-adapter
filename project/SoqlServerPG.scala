import sbt._
import Keys._

import Dependencies._

object SoqlServerPG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings(assembly=true) ++ Seq(
    resourceGenerators in Compile <+= (resourceManaged in Compile, name in Compile, version in Compile, scalaVersion in Compile) map CommonPG.genVersion,
    libraryDependencies ++= libraries()
  )

  def libraries() = Seq(
    secondarylib,
    metricsJetty,
    metricsGraphite
  )
}


