import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  secondarylib,
  socrataHttpCuratorBroker
)

test in assembly := {}

mainClass := Some("com.socrata.pg.server.QueryServer")
