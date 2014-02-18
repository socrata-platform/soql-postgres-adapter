resolvers ++= Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "DiversIT repo" at "http://repository-diversit.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.1.1")

libraryDependencies ++= Seq(
  "com.rojoma" %% "simple-arm" % "[1.1.10,2.0.0)",
  "com.rojoma" %% "rojoma-json" % "[2.0.0,3.0.0)"
)
