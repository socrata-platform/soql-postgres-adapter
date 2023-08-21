import Dependencies._

name := "store-pg"

libraryDependencies ++= Seq(
  secondarylib,
  clojure,
  coordinator % "test"
)

assembly/test := {}

assembly/assemblyJarName := s"${name.value}-assembly.jar"

assembly/assemblyOutputPath := target.value / (assembly/assemblyJarName).value
