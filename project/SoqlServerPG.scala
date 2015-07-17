import Dependencies._
import sbt.Keys._
import sbt._

object SoqlServerPG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    libraryDependencies ++= libraries()
  )

  def libraries() = Seq(
    secondarylib,
    socrataHttpCuratorBroker,
    metricsJetty,
    metricsGraphite
  )
}


