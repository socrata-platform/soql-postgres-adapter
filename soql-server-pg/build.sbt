import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  cloudwatch,
  commonsCollections,
  jedis,
  secondarylib,
  simpleCsv,
  socrataHttpCuratorBroker,
  soqlUtils
)

assembly/test := {}

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value

mainClass := Some("com.socrata.pg.server.QueryServer")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

assembly/assemblyMergeStrategy ~= { old =>
  {
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x => old(x)
  }
}
