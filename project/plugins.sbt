resolvers ++= Seq(
  Resolver.url("socrata", url("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.socrata" % "socrata-sbt-plugins" % "1.6.8")
addSbtPlugin("com.typesafe.sbt" % "sbt-license-report" % "1.0.0")
