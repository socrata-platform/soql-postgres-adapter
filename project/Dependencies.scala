import sbt._

object Dependencies {
  object versions {
    val c3p0 = "0.9.5-pre8"
    val commonsCli = "1.2"
    val commonsCodec = "1.5"
    val commonsIo = "1.4"
    val commonsLang = "2.6"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val liquibaseCore = "2.0.0"
    val liquibasePlugin = "1.9.5.0"
    val postgresql = "9.3-1102-jdbc41"
    val scalaTest = "2.1.0-RC2"
    val simpleArm = "[1.1.10,2.0.0)"
    val slf4j = "1.7.5"
    val socrataUtils = "[0.6.0,1.0.0)"
    val socrataThirdPartyUtil = "[2.4.0,3.0.0)"
    val socrataHttpCuratorBroker = "[2.0.0,3.0.0)"
    val soqlStdlib = "[0.2.1,1.0.0)"
    val typesafeConfig = "1.0.0"
    val dataCoordinator = "0.2.2-SNAPSHOT"
    val typesafeScalaLogging = "1.1.0"
    val rojomaJson = "[2.4.3,3.0.0)"
  }

  val c3p0 = "com.mchange" % "c3p0" % versions.c3p0

  val commonsCli = "commons-cli" % "commons-cli" % versions.commonsCli

  val commonsCodec = "commons-codec" % "commons-codec" % versions.commonsCodec

  val commonsIo = "commons-io" % "commons-io" % versions.commonsIo

  val commonsLang = "commons-lang" % "commons-lang" % versions.commonsLang

  val jodaConvert = "org.joda" % "joda-convert" % versions.jodaConvert

  val jodaTime = "joda-time" % "joda-time" % versions.jodaTime

  val liquibaseCore = "org.liquibase" % "liquibase-core" % versions.liquibaseCore

  val liquibasePlugin = "org.liquibase" % "liquibase-plugin" % versions.liquibasePlugin

  val postgresql = "org.postgresql" % "postgresql" % versions.postgresql

  val scalaTest = "org.scalatest" %% "scalatest" % versions.scalaTest

  val simpleArm = "com.rojoma" %% "simple-arm" % versions.simpleArm

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val socrataThirdPartyUtil = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdPartyUtil

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val secondarylib = "com.socrata" %% "secondarylib" % versions.dataCoordinator // % "provided"
  val coordinatorlib = "com.socrata" %% "coordinator" % versions.dataCoordinator

  val slf4j = "org.slf4j" % "slf4j-log4j12" % versions.slf4j

  val typesafeScalaLogging = "com.typesafe" %% "scalalogging-slf4j" % versions.typesafeScalaLogging

  val rojomaJson = "com.rojoma" %% "rojoma-json" % versions.rojomaJson
}
