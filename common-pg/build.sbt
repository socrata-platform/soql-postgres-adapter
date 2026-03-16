import Dependencies._

name := "common-pg"

libraryDependencies ++= Seq(
  c3p0,
  clojure,
  commonsCodec,
  commonsIo,
  jodaConvert,
  jodaTime,
  liquibaseCore,
  liquibasePlugin,
  postgresql,
  socrataCuratorUtils,
  socrataHttpServer,
  socrataThirdPartyUtils,
  soqlStdlib,
  soqlSqlizer,
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

git.gitUncommittedChanges := false // prevent JGit working-tree inspection, for submodule docker build

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, "revision" -> git.gitHeadCommit.value.getOrElse("unknown")) // when submodule docker build, ignore lack of git info

buildInfoPackage := "com.socrata.pg"

buildInfoOptions += BuildInfoOption.ToJson

disablePlugins(AssemblyPlugin)

Compile/resourceGenerators += Def.task {
  BuildRustStoredProcs(
    (Compile/resourceManaged).value,
    (Compile/resourceDirectory).value,
    (Compile/sourceDirectory).value / "rust"
  )
}.taskValue
