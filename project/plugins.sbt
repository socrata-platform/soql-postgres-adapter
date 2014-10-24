resolvers ++= Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "[1.2.0,2.0.0)",
  "com.rojoma" %% "rojoma-json" % "[2.4.3,3.0.0)"
)
