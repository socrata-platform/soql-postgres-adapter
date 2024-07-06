import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  commonsCollections,
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
