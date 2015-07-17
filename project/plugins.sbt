resolvers ++= Seq(
  "socrata maven" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")
