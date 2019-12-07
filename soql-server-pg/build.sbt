import Dependencies._

name := "soql-server-pg"

libraryDependencies ++= Seq(
  secondarylib,
  socrataHttpCuratorBroker
)

test in assembly := {}
