import Dependencies._

name := "common-pg"

libraryDependencies ++= Seq(
  awsRedshift,
  c3p0,
  clojure,
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
  metricsScala,
  metricsJetty,
  metricsGraphite,
  metricsJmx,
  scalatest % "test",
  rollupMetrics
)

enablePlugins(BuildInfoPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion,git.gitHeadCommit)

buildInfoPackage := "com.socrata.pg"

buildInfoOptions += BuildInfoOption.ToJson

disablePlugins(AssemblyPlugin)
