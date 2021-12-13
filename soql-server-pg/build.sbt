import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  secondarylib,
  socrataHttpCuratorBroker
)

assembly/test := {}

mainClass := Some("com.socrata.pg.server.QueryServer")
