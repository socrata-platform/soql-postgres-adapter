import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  secondarylib,
  socrataHttpCuratorBroker
)

assembly/test := {}

mainClass := Some("com.socrata.pg.server.QueryServer")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
