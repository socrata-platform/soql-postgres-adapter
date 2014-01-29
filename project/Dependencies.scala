import sbt._

object Dependencies {
  object versions {
    val commonsCli = "1.2"
    val commonsCodec = "1.5"
    val commonsIo = "1.4"
    val commonsLang = "2.6"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val scalaTest = "1.9.1"
    val simpleArm = "[1.1.10,2.0.0)"
    val slf4j = "1.7.5"
    val socrataUtils = "[0.6.0,1.0.0)"
    val socrataThirdPartyUtil = "[2.0.0,3.0.0)"
    val socrataHttpCuratorBroker = "2.0.0-SNAPSHOT"
    val soqlStdlib = "0.0.16-SNAPSHOT"
    val typesafeConfig = "1.0.0"
    val dataCoordinator = "0.0.1-SNAPSHOT"
  }

  val commonsCli = "commons-cli" % "commons-cli" % versions.commonsCli

  val commonsCodec = "commons-codec" % "commons-codec" % versions.commonsCodec

  val commonsIo = "commons-io" % "commons-io" % versions.commonsIo

  val commonsLang = "commons-lang" % "commons-lang" % versions.commonsLang

  val jodaConvert = "org.joda" % "joda-convert" % versions.jodaConvert

  val jodaTime = "joda-time" % "joda-time" % versions.jodaTime

  val scalaTest = "org.scalatest" %% "scalatest" % versions.scalaTest

  val simpleArm = "com.rojoma" %% "simple-arm" % versions.simpleArm

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val socrataThirdPartyUtil = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdPartyUtil

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val secondarylib = "com.socrata" %% "secondarylib" % versions.dataCoordinator // % "provided"
  val coordinatorlib = "com.socrata" %% "coordinator" % versions.dataCoordinator

  val slf4j = "org.slf4j" % "slf4j-log4j12" % versions.slf4j
}
