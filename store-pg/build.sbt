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

assembly/assemblyMergeStrategy ~= { old =>
  {
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x => old(x)
  }
}

excludeDependencies ++= Seq(
  ExclusionRule("log4j", "log4j")
)
