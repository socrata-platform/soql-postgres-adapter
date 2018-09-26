resolvers ++= Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "local" at file(Path.userHome.absolutePath + "/.ivy2/local").getAbsolutePath
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.6")
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")
