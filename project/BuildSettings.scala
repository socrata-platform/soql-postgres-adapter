import sbt.Keys._
import sbt._

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Seq(
    fork in test := true,
    scalacOptions ++= Seq("-language:implicitConversions")
  )

  val projectSettings = buildSettings ++
    Seq(
      // TODO: enable style build failures
      com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
      // TODO: enable code coverage build failures
      scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false
    )
}
