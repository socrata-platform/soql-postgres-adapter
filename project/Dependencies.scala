import sbt._

object Dependencies {
  object versions {
    val c3p0 = "0.9.5-pre9"
    val commonsCli = "1.2"
    val commonsCodec = "1.11"
    val commonsCollections = "4.4"
    val commonsIo = "1.4"
    val commonsLang = "2.6"
    val jedis = "4.2.3"
    val jodaConvert = "1.2"
    val jodaTime = "2.1"
    val liquibaseCore = "2.0.0"
    val liquibasePlugin = "1.9.5.0"
    val postgresql = "42.6.0"
    val simpleArm = "1.1.10"
    val slf4j = "1.7.5"
    val scalatest = "3.0.8"
    val socrataUtils = "0.11.0"
    val socrataCuratorUtils = "1.2.0"
    val socrataThirdPartyUtils = "5.0.0"
    val socrataHttp = "3.13.4"
    val soqlStdlib = "4.14.80"
    val simpleCsv = "0.0.2"
    val typesafeConfig = "1.0.0"
    val dataCoordinator = "4.2.43"
    val typesafeScalaLogging = "3.9.2"
    val rojomaJson = "3.13.0"
    val metrics = "4.2.30"
    val metricsScala = "4.3.3"
    val clojure = "1.5.1"
    val rollupMetrics = "4.1"
  }

  val c3p0 = "com.mchange" % "c3p0" % versions.c3p0

  val commonsCli = "commons-cli" % "commons-cli" % versions.commonsCli

  val commonsCodec = "commons-codec" % "commons-codec" % versions.commonsCodec

  val commonsCollections = "org.apache.commons" % "commons-collections4" % versions.commonsCollections

  val commonsIo = "commons-io" % "commons-io" % versions.commonsIo

  val commonsLang = "commons-lang" % "commons-lang" % versions.commonsLang

  val jedis = "redis.clients" % "jedis" % versions.jedis

  val jodaConvert = "org.joda" % "joda-convert" % versions.jodaConvert

  val jodaTime = "joda-time" % "joda-time" % versions.jodaTime

  val liquibaseCore = "org.liquibase" % "liquibase-core" % versions.liquibaseCore

  val liquibasePlugin = "org.liquibase" % "liquibase-plugin" % versions.liquibasePlugin

  val postgresql = "org.postgresql" % "postgresql" % versions.postgresql

  val simpleArm = "com.rojoma" %% "simple-arm" % versions.simpleArm

  val simpleCsv = "com.socrata" %% "simplecsv-scala" % versions.simpleCsv

  val socrataUtil = "com.socrata" %% "socrata-utils" % versions.socrataUtils

  val socrataCuratorUtils = "com.socrata" %% "socrata-curator-utils" % versions.socrataCuratorUtils
  val socrataThirdPartyUtils = "com.socrata" %% "socrata-thirdparty-utils" % versions.socrataThirdPartyUtils

  val socrataHttpCuratorBroker = "com.socrata" %% "socrata-http-curator-broker" % versions.socrataHttp
  val socrataHttpServer = "com.socrata" %% "socrata-http-server" % versions.socrataHttp

  val soqlStdlib = "com.socrata" %% "soql-stdlib" % versions.soqlStdlib
  val soqlUtils = "com.socrata" %% "soql-utils" % versions.soqlStdlib
  val soqlSqlizer = "com.socrata" %% "soql-sqlizer" % versions.soqlStdlib

  val typesafeConfig = "com.typesafe" % "config" % versions.typesafeConfig

  val secondarylib = "com.socrata" %% "secondarylib" % versions.dataCoordinator // % "provided"
  val coordinatorlib = "com.socrata" %% "coordinatorlib" % versions.dataCoordinator
  val coordinator = "com.socrata" %% "coordinator" % versions.dataCoordinator // ugh, this shouldn't be published at all

  val slf4j = "org.slf4j" % "slf4j-log4j12" % versions.slf4j

  val scalatest = "org.scalatest" %% "scalatest" % versions.scalatest

  val typesafeScalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % versions.typesafeScalaLogging

  val rojomaJson = "com.rojoma" %% "rojoma-json-v3" % versions.rojomaJson

  val metricsJetty = "io.dropwizard.metrics" % "metrics-jetty9" % versions.metrics
  val metricsGraphite = "io.dropwizard.metrics" % "metrics-graphite" % versions.metrics
  val metricsJmx = "io.dropwizard.metrics" % "metrics-jmx" % versions.metrics
  val metricsScala = "nl.grons" %% "metrics4-scala" % versions.metricsScala

  val clojure = "org.clojure" % "clojure" % versions.clojure

  val rollupMetrics = "com.socrata" %% "rollup-metrics" % versions.rollupMetrics
}
