import Dependencies._

name := "store-pg"

libraryDependencies ++= Seq(
  secondarylib,
  clojure,
  coordinator % "test"
)

assembly/test := {}
