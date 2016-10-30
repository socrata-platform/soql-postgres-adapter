resolvers ++= Seq(
  "socrata maven" at "https://repo.socrata.com/artifactory/libs-release"
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.5.6")
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")
