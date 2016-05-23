import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys

object BuildSettings {
  val buildSettings: Seq[Setting[_]] = Seq(
    fork in test := true,
    scalacOptions ++= Seq("-language:implicitConversions"),
    // For debugging, disable stylecheck by uncommenting the next line
    // com.socrata.sbtplugins.StylePlugin.StyleKeys.styleFailOnError in Compile := false,
    BuildInfoKeys.buildInfoPackage := "com.socrata.pg"
  )

  val projectSettings = buildSettings ++
    Seq(
      com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Test := {},
      com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Compile := {},
      scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := false
    )
}
