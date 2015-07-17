import Dependencies._
import sbt.Keys._
import sbt._

object CommonPG {
  lazy val settings: Seq[Setting[_]] = BuildSettings.projectSettings ++ Seq(
    resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools",
    libraryDependencies ++= Seq(
      c3p0,
      commonsCodec,
      commonsIo,
      jodaConvert,
      jodaTime,
      liquibaseCore,
      liquibasePlugin,
      postgresql,
      socrataUtil,
      socrataCuratorUtils,
      socrataThirdPartyUtils,
      soqlStdlib,
      secondarylib,
      coordinatorlib,
      typesafeConfig,
      slf4j,
      typesafeScalaLogging,
      rojomaJson,
      metricsScala
    )
  )
}


